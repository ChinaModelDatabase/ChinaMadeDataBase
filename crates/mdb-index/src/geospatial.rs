//! 2dsphere 地理空间索引：包围盒 + 点查询。
//!
//! - GeoJSON 类型：Point, LineString, Polygon
//! - 包围盒索引用于范围查询
//! - `$near` 通过 Haversine 距离排序

use crate::types::RecordId;

const EARTH_RADIUS_METERS: f64 = 6_371_008.8;

// ---------------------------------------------------------------------------
// GeoJSON 类型
// ---------------------------------------------------------------------------

/// 经纬度坐标点（单位：度）。
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct GeoPoint {
    pub lon: f64,
    pub lat: f64,
}

/// GeoJSON 几何类型。
#[derive(Debug, Clone, PartialEq)]
pub enum GeoShape {
    Point(GeoPoint),
    LineString(Vec<GeoPoint>),
    Polygon(Vec<Vec<GeoPoint>>),
}

/// 包围盒。
#[derive(Debug, Clone, Copy)]
pub struct BBox {
    pub min_lon: f64,
    pub min_lat: f64,
    pub max_lon: f64,
    pub max_lat: f64,
}

impl BBox {
    pub fn new(min_lon: f64, min_lat: f64, max_lon: f64, max_lat: f64) -> Self {
        Self { min_lon, min_lat, max_lon, max_lat }
    }

    pub fn contains_point(&self, p: &GeoPoint) -> bool {
        p.lon >= self.min_lon
            && p.lon <= self.max_lon
            && p.lat >= self.min_lat
            && p.lat <= self.max_lat
    }

    pub fn intersects(&self, other: &BBox) -> bool {
        self.min_lon <= other.max_lon
            && self.max_lon >= other.min_lon
            && self.min_lat <= other.max_lat
            && self.max_lat >= other.min_lat
    }
}

/// 为一组点计算包围盒。
pub fn bounding_box(points: &[GeoPoint]) -> Option<BBox> {
    if points.is_empty() {
        return None;
    }
    let mut min_lon = f64::MAX;
    let mut min_lat = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut max_lat = f64::MIN;
    for p in points {
        min_lon = min_lon.min(p.lon);
        min_lat = min_lat.min(p.lat);
        max_lon = max_lon.max(p.lon);
        max_lat = max_lat.max(p.lat);
    }
    Some(BBox::new(min_lon, min_lat, max_lon, max_lat))
}

// ---------------------------------------------------------------------------
// 距离计算
// ---------------------------------------------------------------------------

/// Haversine 距离（单位：米）。
pub fn haversine_distance(a: &GeoPoint, b: &GeoPoint) -> f64 {
    let dlat = (b.lat - a.lat).to_radians();
    let dlon = (b.lon - a.lon).to_radians();
    let lat1 = a.lat.to_radians();
    let lat2 = b.lat.to_radians();

    let h = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * h.sqrt().asin();

    EARTH_RADIUS_METERS * c
}

// ---------------------------------------------------------------------------
// 索引条目
// ---------------------------------------------------------------------------

#[derive(Debug, Clone)]
struct GeoEntry {
    record_id: RecordId,
    shape: GeoShape,
    bbox: BBox,
}

/// 地理空间索引。
#[derive(Debug)]
pub struct GeoIndex {
    name: String,
    entries: Vec<GeoEntry>,
}

impl GeoIndex {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into(), entries: vec![] }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// 计算 GeoShape 的包围盒。
    fn shape_bbox(shape: &GeoShape) -> BBox {
        match shape {
            GeoShape::Point(p) => BBox::new(p.lon, p.lat, p.lon, p.lat),
            GeoShape::LineString(pts) => {
                bounding_box(pts).unwrap_or(BBox::new(0.0, 0.0, 0.0, 0.0))
            }
            GeoShape::Polygon(rings) => {
                let all: Vec<GeoPoint> = rings.iter().flatten().cloned().collect();
                bounding_box(&all).unwrap_or(BBox::new(0.0, 0.0, 0.0, 0.0))
            }
        }
    }

    /// 插入一个地理空间形状。
    pub fn insert(&mut self, shape: GeoShape, record_id: RecordId) {
        let bbox = Self::shape_bbox(&shape);
        self.entries.push(GeoEntry { record_id, shape, bbox });
    }

    /// 移除指定 record_id。
    pub fn remove(&mut self, record_id: RecordId) -> bool {
        let before = self.entries.len();
        self.entries.retain(|e| e.record_id != record_id);
        self.entries.len() < before
    }

    /// `$geoWithin`：返回包围盒与查询区域相交的所有 record_id。
    pub fn geo_within_bbox(&self, query_bbox: &BBox) -> Vec<RecordId> {
        self.entries
            .iter()
            .filter(|e| e.bbox.intersects(query_bbox))
            .map(|e| e.record_id)
            .collect()
    }

    /// `$near`：返回距离 `center` 最近的 record_id，按距离升序排列。
    pub fn near(&self, center: &GeoPoint, max_distance_meters: f64) -> Vec<(RecordId, f64)> {
        let mut results: Vec<(RecordId, f64)> = self
            .entries
            .iter()
            .filter_map(|e| {
                let point = Self::shape_center(&e.shape)?;
                let dist = haversine_distance(center, &point);
                if dist <= max_distance_meters {
                    Some((e.record_id, dist))
                } else {
                    None
                }
            })
            .collect();
        results.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        results
    }

    /// 获取形状的中心点（用于距离计算）。
    fn shape_center(shape: &GeoShape) -> Option<GeoPoint> {
        match shape {
            GeoShape::Point(p) => Some(*p),
            GeoShape::LineString(pts) => {
                if pts.is_empty() {
                    return None;
                }
                let sum_lon: f64 = pts.iter().map(|p| p.lon).sum();
                let sum_lat: f64 = pts.iter().map(|p| p.lat).sum();
                let n = pts.len() as f64;
                Some(GeoPoint { lon: sum_lon / n, lat: sum_lat / n })
            }
            GeoShape::Polygon(rings) => {
                let all: Vec<GeoPoint> = rings.iter().flatten().cloned().collect();
                if all.is_empty() {
                    return None;
                }
                let sum_lon: f64 = all.iter().map(|p| p.lon).sum();
                let sum_lat: f64 = all.iter().map(|p| p.lat).sum();
                let n = all.len() as f64;
                Some(GeoPoint { lon: sum_lon / n, lat: sum_lat / n })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_same_point() {
        let p = GeoPoint { lon: 116.4, lat: 39.9 };
        assert_eq!(haversine_distance(&p, &p), 0.0);
    }

    #[test]
    fn haversine_known_distance() {
        // 北京 (116.4, 39.9) 到 上海 (121.5, 31.2) ≈ 1068 km
        let beijing = GeoPoint { lon: 116.4, lat: 39.9 };
        let shanghai = GeoPoint { lon: 121.5, lat: 31.2 };
        let dist = haversine_distance(&beijing, &shanghai);
        assert!(dist > 1_000_000.0 && dist < 1_200_000.0, "distance = {dist}");
    }

    #[test]
    fn bbox_contains_point() {
        let bbox = BBox::new(100.0, 30.0, 120.0, 40.0);
        assert!(bbox.contains_point(&GeoPoint { lon: 110.0, lat: 35.0 }));
        assert!(!bbox.contains_point(&GeoPoint { lon: 130.0, lat: 35.0 }));
    }

    #[test]
    fn bbox_intersection() {
        let a = BBox::new(100.0, 30.0, 120.0, 40.0);
        let b = BBox::new(110.0, 35.0, 130.0, 45.0);
        let c = BBox::new(200.0, 50.0, 210.0, 60.0);
        assert!(a.intersects(&b));
        assert!(!a.intersects(&c));
    }

    #[test]
    fn geo_within_bbox() {
        let mut idx = GeoIndex::new("geo_test");
        idx.insert(GeoShape::Point(GeoPoint { lon: 116.4, lat: 39.9 }), 1);
        idx.insert(GeoShape::Point(GeoPoint { lon: 121.5, lat: 31.2 }), 2);
        idx.insert(GeoShape::Point(GeoPoint { lon: 113.3, lat: 23.1 }), 3);

        let query = BBox::new(115.0, 30.0, 125.0, 45.0);
        let mut result = idx.geo_within_bbox(&query);
        result.sort();
        assert_eq!(result, vec![1, 2]);
    }

    #[test]
    fn near_query() {
        let mut idx = GeoIndex::new("geo_test");
        idx.insert(GeoShape::Point(GeoPoint { lon: 116.4, lat: 39.9 }), 1);
        idx.insert(GeoShape::Point(GeoPoint { lon: 121.5, lat: 31.2 }), 2);
        idx.insert(GeoShape::Point(GeoPoint { lon: 113.3, lat: 23.1 }), 3);

        let center = GeoPoint { lon: 116.4, lat: 39.9 };
        let results = idx.near(&center, 2_000_000.0); // 2000km
        // 北京应该在范围内
        assert!(!results.is_empty());
        assert_eq!(results[0].0, 1); // 最近的是北京本身
    }

    #[test]
    fn remove_record() {
        let mut idx = GeoIndex::new("geo_test");
        idx.insert(GeoShape::Point(GeoPoint { lon: 116.4, lat: 39.9 }), 1);
        idx.insert(GeoShape::Point(GeoPoint { lon: 121.5, lat: 31.2 }), 2);
        assert!(idx.remove(1));
        assert_eq!(idx.len(), 1);
    }

    #[test]
    fn polygon_bbox() {
        let ring = vec![
            GeoPoint { lon: 0.0, lat: 0.0 },
            GeoPoint { lon: 10.0, lat: 0.0 },
            GeoPoint { lon: 10.0, lat: 10.0 },
            GeoPoint { lon: 0.0, lat: 10.0 },
            GeoPoint { lon: 0.0, lat: 0.0 },
        ];
        let shape = GeoShape::Polygon(vec![ring]);
        let bbox = GeoIndex::shape_bbox(&shape);
        assert_eq!(bbox.min_lon, 0.0);
        assert_eq!(bbox.max_lon, 10.0);
        assert_eq!(bbox.min_lat, 0.0);
        assert_eq!(bbox.max_lat, 10.0);
    }
}
