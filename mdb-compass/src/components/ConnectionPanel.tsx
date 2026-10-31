import React from 'react';
import { Card, Input, Button, Switch, Form, Space, Tag, Alert } from 'antd';
import { ApiOutlined, DisconnectOutlined, LoadingOutlined } from '@ant-design/icons';
import { useConnectionStore } from '../stores/connectionStore';

const ConnectionPanel: React.FC = () => {
  const {
    host, port, username, password, authSource, tls,
    connected, connecting, error,
    setHost, setPort, setUsername, setPassword, setAuthSource, setTls,
    connect, disconnect,
  } = useConnectionStore();

  const handleConnect = async () => {
    if (connected) {
      await disconnect();
    } else {
      await connect();
    }
  };

  return (
    <Card
      title={
        <Space>
          <ApiOutlined />
          <span>Connection</span>
        </Space>
      }
      size="small"
      className="connection-panel"
    >
      {error && <Alert message={error} type="error" showIcon closable style={{ marginBottom: 12 }} />}

      <Form layout="vertical" size="small">
        <Form.Item label="Host">
          <Input
            value={host}
            onChange={(e) => setHost(e.target.value)}
            placeholder="localhost"
            disabled={connected}
          />
        </Form.Item>
        <Form.Item label="Port">
          <Input
            type="number"
            value={port}
            onChange={(e) => setPort(parseInt(e.target.value) || 27017)}
            placeholder="27017"
            disabled={connected}
          />
        </Form.Item>
        <Form.Item label="Username">
          <Input
            value={username}
            onChange={(e) => setUsername(e.target.value)}
            placeholder="Optional"
            disabled={connected}
          />
        </Form.Item>
        <Form.Item label="Password">
          <Input.Password
            value={password}
            onChange={(e) => setPassword(e.target.value)}
            placeholder="Optional"
            disabled={connected}
          />
        </Form.Item>
        <Form.Item label="Auth Source">
          <Input
            value={authSource}
            onChange={(e) => setAuthSource(e.target.value)}
            placeholder="admin"
            disabled={connected}
          />
        </Form.Item>
        <Form.Item label="TLS">
          <Switch
            checked={tls}
            onChange={(checked) => setTls(checked)}
            disabled={connected}
          />
        </Form.Item>
        <Form.Item>
          <Button
            type="primary"
            icon={connecting ? <LoadingOutlined /> : connected ? <DisconnectOutlined /> : <ApiOutlined />}
            onClick={handleConnect}
            loading={connecting}
            block
            danger={connected}
          >
            {connected ? 'Disconnect' : 'Connect'}
          </Button>
        </Form.Item>
      </Form>

      {connected && (
        <Tag color="success" style={{ width: '100%', textAlign: 'center', padding: 4 }}>
          Connected to {host}:{port}
        </Tag>
      )}
    </Card>
  );
};

export default ConnectionPanel;
