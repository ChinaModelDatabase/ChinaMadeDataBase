import React, { useState, useEffect } from 'react';
import { Card, Table, Button, Space, Modal, Form, Input, Select, Switch, InputNumber, Tag, message, Empty, Popconfirm } from 'antd';
import { PlusOutlined, DeleteOutlined, ReloadOutlined } from '@ant-design/icons';
import { useDatabaseStore } from '../stores/databaseStore';
import { mdbService } from '../services/mdbService';

const { Option } = Select;

interface IndexInfo {
  name: string;
  key: Record<string, unknown>;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
  v?: number;
}

const IndexManager: React.FC = () => {
  const { currentDb, currentCollection, indexes, indexesLoading, refreshIndexes } = useDatabaseStore();
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [indexFields, setIndexFields] = useState<{ field: string; direction: 1 | -1 }[]>([{ field: '', direction: 1 }]);
  const [unique, setUnique] = useState(false);
  const [sparse, setSparse] = useState(false);
  const [ttl, setTtl] = useState<number | null>(null);
  const [indexName, setIndexName] = useState('');
  const [creating, setCreating] = useState(false);

  useEffect(() => {
    if (currentDb && currentCollection) {
      refreshIndexes(currentDb, currentCollection);
    }
  }, [currentDb, currentCollection, refreshIndexes]);

  const handleCreateIndex = async () => {
    if (!currentDb || !currentCollection) return;

    const keys: Record<string, 1 | -1> = {};
    for (const field of indexFields) {
      if (field.field) {
        keys[field.field] = field.direction;
      }
    }

    if (Object.keys(keys).length === 0) {
      message.warning('Add at least one field');
      return;
    }

    setCreating(true);
    const options: Record<string, unknown> = {};
    if (indexName) options.name = indexName;
    if (unique) options.unique = true;
    if (sparse) options.sparse = true;
    if (ttl !== null && ttl > 0) options.expireAfterSeconds = ttl;

    const result = await mdbService.createIndex(currentDb, currentCollection, keys, options);
    setCreating(false);

    if (result.error) {
      message.error(result.error);
    } else {
      message.success(`Index "${result.indexName}" created`);
      setCreateModalVisible(false);
      setIndexFields([{ field: '', direction: 1 }]);
      setUnique(false);
      setSparse(false);
      setTtl(null);
      setIndexName('');
      refreshIndexes(currentDb, currentCollection);
    }
  };

  const handleDropIndex = async (indexName: string) => {
    if (!currentDb || !currentCollection) return;

    const result = await mdbService.dropIndex(currentDb, currentCollection, indexName);
    if (result.error) {
      message.error(result.error);
    } else {
      message.success(`Index "${indexName}" dropped`);
      refreshIndexes(currentDb, currentCollection);
    }
  };

  const addIndexField = () => {
    setIndexFields([...indexFields, { field: '', direction: 1 }]);
  };

  const removeIndexField = (idx: number) => {
    setIndexFields(indexFields.filter((_, i) => i !== idx));
  };

  const updateIndexField = (idx: number, updates: Partial<{ field: string; direction: 1 | -1 }>) => {
    setIndexFields(indexFields.map((f, i) => (i === idx ? { ...f, ...updates } : f)));
  };

  const columns = [
    {
      title: 'Name',
      dataIndex: 'name',
      key: 'name',
      render: (name: string) => <Tag>{name}</Tag>,
    },
    {
      title: 'Key',
      dataIndex: 'key',
      key: 'key',
      render: (key: Record<string, unknown>) => (
        <Space>
          {Object.entries(key).map(([field, value]) => (
            <Tag key={field} color="blue">
              {field}: {String(value)}
            </Tag>
          ))}
        </Space>
      ),
    },
    {
      title: 'Unique',
      dataIndex: 'unique',
      key: 'unique',
      width: 80,
      render: (unique: boolean) => unique ? <Tag color="green">Yes</Tag> : <Tag>No</Tag>,
    },
    {
      title: 'Sparse',
      dataIndex: 'sparse',
      key: 'sparse',
      width: 80,
      render: (sparse: boolean) => sparse ? <Tag color="orange">Yes</Tag> : <Tag>No</Tag>,
    },
    {
      title: 'TTL',
      dataIndex: 'expireAfterSeconds',
      key: 'expireAfterSeconds',
      width: 80,
      render: (ttl: number) => ttl ? `${ttl}s` : '-',
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 80,
      render: (_: unknown, record: IndexInfo) => (
        record.name === '_id_' ? null : (
          <Popconfirm
            title="Drop this index?"
            onConfirm={() => handleDropIndex(record.name)}
            okText="Drop"
            okButtonProps={{ danger: true }}
          >
            <Button type="text" danger size="small" icon={<DeleteOutlined />} />
          </Popconfirm>
        )
      ),
    },
  ];

  if (!currentDb || !currentCollection) {
    return (
      <Card title="Indexes" size="small" className="index-manager">
        <Empty description="Select a collection to manage indexes" />
      </Card>
    );
  }

  return (
    <Card
      title="Indexes"
      size="small"
      className="index-manager"
      extra={
        <Space>
          <Button size="small" icon={<ReloadOutlined />} onClick={() => refreshIndexes(currentDb, currentCollection)} loading={indexesLoading}>
            Refresh
          </Button>
          <Button size="small" type="primary" icon={<PlusOutlined />} onClick={() => setCreateModalVisible(true)}>
            Create Index
          </Button>
        </Space>
      }
    >
      <Table
        columns={columns}
        dataSource={indexes.map((idx, i) => ({ ...idx, _key: idx.name || i }))}
        rowKey="_key"
        loading={indexesLoading}
        size="small"
        pagination={false}
        locale={{ emptyText: 'No indexes' }}
      />

      <Modal
        title="Create Index"
        open={createModalVisible}
        onOk={handleCreateIndex}
        onCancel={() => setCreateModalVisible(false)}
        confirmLoading={creating}
        okText="Create"
      >
        <Form layout="vertical">
          <Form.Item label="Index Name (optional)">
            <Input value={indexName} onChange={(e) => setIndexName(e.target.value)} placeholder="Auto-generated" />
          </Form.Item>

          <Form.Item label="Fields">
            {indexFields.map((field, idx) => (
              <Space key={idx} style={{ display: 'flex', marginBottom: 8 }}>
                <Input
                  placeholder="Field name"
                  value={field.field}
                  onChange={(e) => updateIndexField(idx, { field: e.target.value })}
                  style={{ width: 200 }}
                />
                <Select
                  value={field.direction}
                  onChange={(value) => updateIndexField(idx, { direction: value })}
                  style={{ width: 150 }}
                >
                  <Option value={1}>Ascending (1)</Option>
                  <Option value={-1}>Descending (-1)</Option>
                </Select>
                {indexFields.length > 1 && (
                  <Button type="text" danger icon={<DeleteOutlined />} onClick={() => removeIndexField(idx)} />
                )}
              </Space>
            ))}
            <Button type="dashed" onClick={addIndexField} icon={<PlusOutlined />} block>
              Add Field
            </Button>
          </Form.Item>

          <Form.Item label="Options">
            <Space>
              <div>
                <Switch checked={unique} onChange={setUnique} /> <span>Unique</span>
              </div>
              <div>
                <Switch checked={sparse} onChange={setSparse} /> <span>Sparse</span>
              </div>
              <div>
                <InputNumber
                  min={0}
                  placeholder="TTL (seconds)"
                  value={ttl}
                  onChange={(value) => setTtl(value)}
                  style={{ width: 150 }}
                />
              </div>
            </Space>
          </Form.Item>
        </Form>
      </Modal>
    </Card>
  );
};

export default IndexManager;
