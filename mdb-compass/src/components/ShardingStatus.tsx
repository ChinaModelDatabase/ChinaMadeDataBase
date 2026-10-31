import React, { useState, useEffect, useCallback } from 'react';
import { Card, Table, Tag, Empty, Spin, Button, Descriptions } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useConnectionStore } from '../stores/connectionStore';
import { mdbService } from '../services/mdbService';

interface ShardInfo {
  _id: string;
  host: string;
  state: number;
}

interface ShardStatusData {
  shards: ShardInfo[];
  ok: number;
}

const ShardingStatus: React.FC = () => {
  const { connected } = useConnectionStore();
  const [status, setStatus] = useState<ShardStatusData | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = useCallback(async () => {
    if (!connected) return;
    setLoading(true);
    setError(null);
    try {
      const result = await mdbService.shardStatus();
      if (result.error) {
        setError(result.error);
      } else if (result.status) {
        setStatus(result.status as unknown as ShardStatusData);
      }
    } catch (err) {
      setError(String(err));
    }
    setLoading(false);
  }, [connected]);

  useEffect(() => {
    fetchStatus();
  }, [fetchStatus]);

  if (!connected) {
    return (
      <Card title="Sharding Status" size="small">
        <Empty description="Connect to a database first" />
      </Card>
    );
  }

  if (loading && !status) {
    return (
      <Card title="Sharding Status" size="small">
        <Spin tip="Loading..." />
      </Card>
    );
  }

  if (error) {
    return (
      <Card title="Sharding Status" size="small">
        <Empty description={`Error: ${error}`} />
      </Card>
    );
  }

  if (!status || !status.shards || status.shards.length === 0) {
    return (
      <Card title="Sharding Status" size="small">
        <Empty description="No sharding information available. This may not be a sharded cluster." />
      </Card>
    );
  }

  const columns = [
    {
      title: 'Shard ID',
      dataIndex: '_id',
      key: '_id',
      render: (id: string) => <Tag color="blue">{id}</Tag>,
    },
    {
      title: 'Host',
      dataIndex: 'host',
      key: 'host',
      render: (host: string) => <code>{host}</code>,
    },
    {
      title: 'State',
      dataIndex: 'state',
      key: 'state',
      render: (state: number) => (
        <Tag color={state === 1 ? 'green' : 'orange'}>
          {state === 1 ? 'Running' : `State ${state}`}
        </Tag>
      ),
    },
  ];

  return (
    <Card
      title="Sharding Status"
      size="small"
      extra={
        <Button size="small" icon={<ReloadOutlined />} onClick={fetchStatus} loading={loading}>
          Refresh
        </Button>
      }
    >
      <Descriptions size="small" column={2} style={{ marginBottom: 16 }}>
        <Descriptions.Item label="Total Shards">{status.shards.length}</Descriptions.Item>
        <Descriptions.Item label="OK">{status.ok ? 'Yes' : 'No'}</Descriptions.Item>
      </Descriptions>

      <Table
        columns={columns}
        dataSource={status.shards}
        rowKey="_id"
        size="small"
        pagination={false}
      />
    </Card>
  );
};

export default ShardingStatus;
