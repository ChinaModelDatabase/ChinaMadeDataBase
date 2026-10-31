import React, { useState, useEffect, useCallback } from 'react';
import { Card, Empty, Spin, Tag, Space, Row, Col, Statistic } from 'antd';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { useConnectionStore } from '../stores/connectionStore';
import { mdbService } from '../services/mdbService';

interface Metrics {
  connections: number;
  opsPerSec: number;
  storageUsed: number;
  storageTotal: number;
  timestamp: number;
}

const COLORS = ['#13c2c2', '#faad14', '#f5222d', '#52c41a', '#722ed1', '#1890ff'];

const PerformanceCharts: React.FC = () => {
  const { connected } = useConnectionStore();
  const [metrics, setMetrics] = useState<Metrics[]>([]);
  const [currentStats, setCurrentStats] = useState<Record<string, unknown> | null>(null);
  const [loading, setLoading] = useState(false);

  const fetchMetrics = useCallback(async () => {
    if (!connected) return;

    try {
      const result = await mdbService.serverStatus();
      if (result.status) {
        const status = result.status;
        const connections = (status as Record<string, unknown>).connections as Record<string, unknown> | undefined;
        const opcounters = (status as Record<string, unknown>).opcounters as Record<string, unknown> | undefined;
        const storage = (status as Record<string, unknown>).wiredTiger as Record<string, unknown> | undefined;

        const connCount = connections?.current as number || 0;
        let totalOps = 0;
        if (opcounters) {
          for (const [, value] of Object.entries(opcounters)) {
            totalOps += (value as number) || 0;
          }
        }

        const cache = storage?.cache as Record<string, unknown> | undefined;
        const bytesCurrentlyInCache = cache?.["bytes currently in the cache"] as number || 0;
        const maximumBytesConfigured = cache?.["maximum bytes configured"] as number || 1;

        const newMetric: Metrics = {
          connections: connCount,
          opsPerSec: totalOps,
          storageUsed: bytesCurrentlyInCache,
          storageTotal: maximumBytesConfigured,
          timestamp: Date.now(),
        };

        setMetrics((prev) => [...prev.slice(-19), newMetric]);
        setCurrentStats(status);
      }
    } catch {
      // ignore errors for polling
    }
  }, [connected]);

  useEffect(() => {
    if (!connected) {
      setMetrics([]);
      setCurrentStats(null);
      return;
    }

    fetchMetrics();
    const interval = setInterval(fetchMetrics, 5000);
    return () => clearInterval(interval);
  }, [connected, fetchMetrics]);

  if (!connected) {
    return (
      <Card title="Performance" size="small" className="performance-charts">
        <Empty description="Connect to a database to view performance metrics" />
      </Card>
    );
  }

  if (metrics.length === 0) {
    return (
      <Card title="Performance" size="small" className="performance-charts">
        <Spin tip="Collecting metrics..." />
      </Card>
    );
  }

  const latest = metrics[metrics.length - 1];
  const connectionsData = metrics.map((m) => ({
    time: new Date(m.timestamp).toLocaleTimeString(),
    connections: m.connections,
  }));

  const storageData = [
    { name: 'Used', value: latest.storageUsed },
    { name: 'Free', value: latest.storageTotal - latest.storageUsed },
  ];

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  return (
    <Card title="Performance" size="small" className="performance-charts">
      <Row gutter={16} style={{ marginBottom: 16 }}>
        <Col span={8}>
          <Statistic title="Current Connections" value={latest.connections} />
        </Col>
        <Col span={8}>
          <Statistic title="Storage Used" value={formatBytes(latest.storageUsed)} />
        </Col>
        <Col span={8}>
          <Statistic title="Storage Total" value={formatBytes(latest.storageTotal)} />
        </Col>
      </Row>

      <Row gutter={16}>
        <Col span={14}>
          <Card title="Connections Over Time" size="small">
            <ResponsiveContainer width="100%" height={200}>
              <LineChart data={connectionsData}>
                <CartesianGrid strokeDasharray="3 3" stroke="#333" />
                <XAxis dataKey="time" stroke="#888" tick={{ fontSize: 10 }} />
                <YAxis stroke="#888" tick={{ fontSize: 10 }} />
                <Tooltip contentStyle={{ backgroundColor: '#1a1a2e', border: '1px solid #333' }} />
                <Line type="monotone" dataKey="connections" stroke="#13c2c2" strokeWidth={2} dot={false} />
              </LineChart>
            </ResponsiveContainer>
          </Card>
        </Col>
        <Col span={10}>
          <Card title="Storage Distribution" size="small">
            <ResponsiveContainer width="100%" height={200}>
              <PieChart>
                <Pie
                  data={storageData}
                  cx="50%"
                  cy="50%"
                  innerRadius={50}
                  outerRadius={80}
                  paddingAngle={2}
                  dataKey="value"
                >
                  {storageData.map((_, index) => (
                    <Cell key={`cell-${index}`} fill={COLORS[index % COLORS.length]} />
                  ))}
                </Pie>
                <Tooltip formatter={(value: number) => formatBytes(value)} />
              </PieChart>
            </ResponsiveContainer>
            <Space>
              <Tag color="#13c2c2">Used: {formatBytes(latest.storageUsed)}</Tag>
              <Tag color="#333">Free: {formatBytes(latest.storageTotal - latest.storageUsed)}</Tag>
            </Space>
          </Card>
        </Col>
      </Row>

      {currentStats && (
        <Card title="Server Info" size="small" style={{ marginTop: 16 }}>
          <Space direction="vertical" style={{ width: '100%' }}>
            {(() => {
              const status = currentStats as Record<string, unknown>;
              const host = status.host as string;
              const version = status.version as string;
              const uptime = status.uptime as number;
              return (
                <>
                  {host && <div><strong>Host:</strong> {host}</div>}
                  {version && <div><strong>Version:</strong> {version}</div>}
                  {uptime && <div><strong>Uptime:</strong> {Math.floor(uptime / 3600)}h {Math.floor((uptime % 3600) / 60)}m</div>}
                </>
              );
            })()}
          </Space>
        </Card>
      )}
    </Card>
  );
};

export default PerformanceCharts;
