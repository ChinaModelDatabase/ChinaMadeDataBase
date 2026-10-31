import React, { useState } from 'react';
import { Card, Table, Button, Space, Input, Tag, message, Empty, Typography, Pagination } from 'antd';
import { CopyOutlined, ReloadOutlined, EditOutlined, CheckOutlined, CloseOutlined } from '@ant-design/icons';
import { useQueryStore } from '../stores/queryStore';
import { useDatabaseStore } from '../stores/databaseStore';

const { Text } = Typography;

const DocumentViewer: React.FC = () => {
  const { results, total, loading, filter, limit, skip, setFilter, setLimit, setSkip, executeQuery } = useQueryStore();
  const { currentDb, currentCollection } = useDatabaseStore();
  const [editingCell, setEditingCell] = useState<{ rowIndex: number; colKey: string } | null>(null);
  const [editValue, setEditValue] = useState('');

  const handleCopy = (text: string) => {
    navigator.clipboard.writeText(text);
    message.success('Copied to clipboard');
  };

  const handleRefresh = () => {
    if (currentDb && currentCollection) {
      executeQuery(currentDb, currentCollection);
    }
  };

  const formatValue = (value: unknown): string => {
    if (value === null || value === undefined) return 'null';
    if (typeof value === 'object') {
      try {
        return JSON.stringify(value, null, 2);
      } catch {
        return String(value);
      }
    }
    return String(value);
  };

  const flattenObject = (obj: Record<string, unknown>, prefix = ''): Record<string, unknown> => {
    const result: Record<string, unknown> = {};
    for (const [key, value] of Object.entries(obj)) {
      const fullKey = prefix ? `${prefix}.${key}` : key;
      if (value && typeof value === 'object' && !Array.isArray(value)) {
        Object.assign(result, flattenObject(value as Record<string, unknown>, fullKey));
      } else {
        result[fullKey] = value;
      }
    }
    return result;
  };

  const columns = results.length > 0
    ? Object.keys(flattenObject(results[0] as Record<string, unknown>)).map((key) => ({
        title: key,
        dataIndex: key,
        key,
        width: 150,
        render: (value: unknown) => {
          const strValue = formatValue(value);
          const truncated = strValue.length > 100 ? strValue.substring(0, 100) + '...' : strValue;
          return (
            <div className="document-cell">
              <Text
                ellipsis
                style={{ cursor: 'pointer', fontFamily: 'monospace', fontSize: 12 }}
                title={strValue}
              >
                {truncated}
              </Text>
              <Button
                type="text"
                size="small"
                icon={<CopyOutlined />}
                onClick={() => handleCopy(strValue)}
                className="copy-btn"
              />
            </div>
          );
        },
      }))
    : [];

  if (!currentDb || !currentCollection) {
    return (
      <Card title="Documents" size="small" className="document-viewer">
        <Empty description="Select a collection to view documents" />
      </Card>
    );
  }

  return (
    <Card
      title={
        <Space>
          <span>Documents</span>
          <Tag>{total} total</Tag>
        </Space>
      }
      size="small"
      className="document-viewer"
      extra={
        <Space>
          <Button size="small" icon={<ReloadOutlined />} onClick={handleRefresh} loading={loading}>
            Refresh
          </Button>
        </Space>
      }
    >
      <div style={{ marginBottom: 8, display: 'flex', gap: 8 }}>
        <Input.TextArea
          placeholder='Filter: { "field": "value" }'
          value={filter}
          onChange={(e) => setFilter(e.target.value)}
          autoSize={{ minRows: 1, maxRows: 3 }}
          style={{ flex: 1 }}
        />
        <Button type="primary" onClick={handleRefresh} loading={loading}>
          Find
        </Button>
      </div>

      <Table
        columns={columns}
        dataSource={results.map((doc, idx) => ({ ...doc, _rowKey: idx }))}
        rowKey="_rowKey"
        loading={loading}
        size="small"
        scroll={{ x: 'max-content', y: 400 }}
        pagination={false}
        locale={{ emptyText: 'No documents found' }}
      />

      <div style={{ marginTop: 8, display: 'flex', justifyContent: 'center' }}>
        <Pagination
          current={Math.floor(skip / limit) + 1}
          pageSize={limit}
          total={total}
          showSizeChanger
          pageSizeOptions={['10', '20', '50', '100']}
          onChange={(page, pageSize) => {
            setSkip((page - 1) * pageSize);
            setLimit(pageSize);
          }}
          showTotal={(total) => `Total ${total} documents`}
        />
      </div>
    </Card>
  );
};

export default DocumentViewer;
