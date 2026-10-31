import React, { useState, useEffect, useCallback } from 'react';
import { Card, Table, Tag, Empty, Spin, Button, Descriptions } from 'antd';
import { ReloadOutlined } from '@ant-design/icons';
import { useConnectionStore } from '../stores/connectionStore';
import { mdbService } from '../services/mdbService';

interface ReplicaMember {
  _id: number;
  name: string;
  health: number;
  state: number;
  stateStr: string;
  uptime: number;
  optime: { ts: { t: number; i: number }; t: number };
  optimeDate: string;
  lastHeartbeat: string;
  lastHeartbeatRecv: string;
  pingMs: number;
  syncSourceHost: string;
}

interface ReplicaStatus {
  set: string;
  myState: number;
  members: ReplicaMember[];
  ok: number;
}

const ReplicaSetStatus: React.FC = () => {
  const { connected } = useConnectionStore();
  const [status, setStatus] = useState<ReplicaStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const fetchStatus = useCallback(async () => {
    if (!connected) return;
    setLoading(true);
    setError(null);
    try {
      const result = await mdbService.replSetGetStatus();
      if (result.error) {
        setError(result.error);
      } else if (result.status) {
        setStatus(result.status as unknown as ReplicaStatus);
      }
    } catch (err) {
      setError(String(err));
    }
    setLoading(false);
  }, [connected]);

  useEffect(() => {
    fetchStatus();
    const interval = setInterval(fetchStatus, 10000);
    return () => clearInterval(interval);
  }, [fetchStatus]);

  if (!connected) {
    return (
      <Card title="Replica Set Status" size="small">
        <Empty description="Connect to a database first" />
      </Card>
    );
  }

  if (loading && !status) {
    return (
      <Card title="Replica Set Status" size="small">
        <Spin tip="Loading..." />
      </Card>
    );
  }

  if (error) {
    return (
      <Card title="Replica Set Status" size="small">
        <Empty description={`Error: ${error}`} />
      </Card>
    );
  }

  if (!status) {
    return (
      <Card title="Replica Set Status" size="small">
        <Empty description="No replica set information available" />
      </Card>
    );
  }

  const getStateTag = (state: number, stateStr: string) => {
    switch (state) {
      case 1: return <Tag color="green">{stateStr}</Tag>;
      case 2: return <Tag color="blue">{stateStr}</Tag>;
      case 7: return <Tag color="orange">{stateStr}</Tag>;
      case 8: return <Tag color="red">{stateStr}</Tag>;
      case 10: return <Tag color="default">{stateStr}</Tag>;
      default: return <Tag>{stateStr}</Tag>;
    }
  };

  const columns = [
    {
      title: 'ID',
      dataIndex: '_id',
      key: '_id',
      width: 60,
    },
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => <code>{name}</code>,
    },
    {
      title: 'State',
      dataIndex: 'stateStr',
      key: 'stateStr',
      render: (stateStr: string, record: ReplicaMember) => getStateTag(record.state, stateStr),
    },
    {
      title: 'Health',
      dataIndex: 'health',
      key: 'health',
      width: 80,
      render: (health: number) => health ? <Tag color="green">OK</Tag> : <Tag color="red">Down</Tag>,
    },
    {
      title: 'Uptime',
      dataIndex: 'uptime',
      key: 'uptime',
      width: 100,
      render: (uptime: number) => {
        const hours = Math.floor(uptime / 3600);
        const minutes = Math.floor((uptime % 3600) / 60);
        return `${hours}h ${minutes}m`;
      },
    },
    {
      title: 'Ping (ms)',
      dataIndex: 'pingMs',
      key: 'pingMs',
      width: 100,
      render: (ping: number) => ping ? `${ping.toFixed(1)}ms` : '-',
    },
    {
      title: 'Sync Source',
      dataIndex: 'syncSourceHost',
      key: 'syncSourceHost',
      render: (host: string) => host || '-',
    },
  ];

  return (
    <Card
      title={`Replica Set: ${status.set}`}
      size="small"
      extra={
        <Button size="small" icon={<ReloadOutlined />} onClick={fetchStatus} loading={loading}>
          Refresh
        </Button>
      }
    >
      <Descriptions size="small" column={3} style={{ marginBottom: 16 }}>
        <Descriptions.Item label="Set Name">{status.set}</Descriptions.Item>
        <Descriptions.Item label="My State">{status.myState}</Descriptions.Item>
        <Descriptions.Item label="OK">{status.ok ? 'Yes' : 'No'}</Descriptions.Item>
      </Descriptions>

      <Table
        columns={columns}
        dataSource={status.members}
        rowKey="_id"
        size="small"
        pagination={false}
      />
    </Card>
  );
};

export default ReplicaSetStatus;
