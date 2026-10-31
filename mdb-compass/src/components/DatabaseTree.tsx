import React, { useEffect, useState } from 'react';
import { Tree, Card, Button, Space, Modal, Input, Spin, Empty, Dropdown } from 'antd';
import {
  DatabaseOutlined,
  FolderOutlined,
  IndexOutlined,
  ReloadOutlined,
  PlusOutlined,
  DeleteOutlined,
} from '@ant-design/icons';
import type { DataNode, TreeProps } from 'antd/es/tree';
import { useConnectionStore } from '../stores/connectionStore';
import { useDatabaseStore } from '../stores/databaseStore';

const DatabaseTree: React.FC = () => {
  const { connected, databases, refreshDatabases } = useConnectionStore();
  const {
    currentDb, currentCollection,
    collections, collectionsLoading,
    selectDatabase, selectCollection,
    refreshCollections, createCollection, dropCollection,
  } = useDatabaseStore();

  const [expandedKeys, setExpandedKeys] = useState<React.Key[]>([]);
  const [createModalVisible, setCreateModalVisible] = useState(false);
  const [newCollectionName, setNewCollectionName] = useState('');
  const [targetDb, setTargetDb] = useState('');
  const [deleteConfirmVisible, setDeleteConfirmVisible] = useState(false);
  const [targetDeleteColl, setTargetDeleteColl] = useState({ db: '', coll: '' });

  useEffect(() => {
    if (connected) {
      refreshDatabases();
    }
  }, [connected, refreshDatabases]);

  const handleRefresh = async () => {
    await refreshDatabases();
  };

  const handleCreateCollection = async () => {
    if (targetDb && newCollectionName) {
      const success = await createCollection(targetDb, newCollectionName);
      if (success) {
        setCreateModalVisible(false);
        setNewCollectionName('');
      }
    }
  };

  const handleDeleteCollection = async () => {
    const { db, coll } = targetDeleteColl;
    if (db && coll) {
      const success = await dropCollection(db, coll);
      if (success) {
        setDeleteConfirmVisible(false);
        setTargetDeleteColl({ db: '', coll: '' });
      }
    }
  };

  const handleExpand = async (keys: React.Key[], { node, expanded }: { node: DataNode; expanded: boolean }) => {
    setExpandedKeys(keys);
    if (expanded) {
      const dbName = node.key as string;
      await refreshCollections(dbName);
    }
  };

  const handleSelect: TreeProps['onSelect'] = (selectedKeys) => {
    if (selectedKeys.length === 0) return;
    const key = selectedKeys[0] as string;
    const parts = key.split('::');
    if (parts.length === 1) {
      selectDatabase(parts[0]);
    } else if (parts.length === 2) {
      selectCollection(parts[0], parts[1]);
    }
  };

  const onRightClick: TreeProps['onRightClick'] = ({ node }) => {
    const key = node.key as string;
    const parts = key.split('::');
    if (parts.length === 1) {
      // Database node
      return {
        event: {} as React.MouseEvent,
        node,
      };
    }
    return undefined;
  };

  const getTreeData = (): DataNode[] => {
    return databases.map((db) => ({
      title: (
        <Dropdown
          menu={{
            items: [
              {
                key: 'create',
                icon: <PlusOutlined />,
                label: 'Create Collection',
                onClick: () => {
                  setTargetDb(db.name);
                  setCreateModalVisible(true);
                },
              },
              {
                key: 'refresh',
                icon: <ReloadOutlined />,
                label: 'Refresh',
                onClick: () => refreshCollections(db.name),
              },
            ],
          }}
          trigger={['contextMenu']}
        >
          <span>{db.name}</span>
        </Dropdown>
      ),
      key: db.name,
      icon: <DatabaseOutlined style={{ color: '#13c2c2' }} />,
      children: collections
        .filter(() => currentDb === db.name)
        .map((coll) => ({
          title: (
            <Dropdown
              menu={{
                items: [
                  {
                    key: 'delete',
                    icon: <DeleteOutlined />,
                    label: 'Drop Collection',
                    danger: true,
                    onClick: () => {
                      setTargetDeleteColl({ db: db.name, coll: coll.name });
                      setDeleteConfirmVisible(true);
                    },
                  },
                ],
              }}
              trigger={['contextMenu']}
            >
              <span>{coll.name}</span>
            </Dropdown>
          ),
          key: `${db.name}::${coll.name}`,
          icon: <FolderOutlined style={{ color: '#faad14' }} />,
          isLeaf: true,
        })),
    }));
  };

  if (!connected) {
    return (
      <Card title="Database Tree" size="small" className="database-tree">
        <Empty description="Connect to a database first" image={Empty.PRESENTED_IMAGE_SIMPLE} />
      </Card>
    );
  }

  return (
    <Card
      title="Database Tree"
      size="small"
      className="database-tree"
      extra={
        <Button
          type="text"
          size="small"
          icon={<ReloadOutlined />}
          onClick={handleRefresh}
          loading={collectionsLoading}
        />
      }
    >
      {collectionsLoading && databases.length === 0 ? (
        <Spin size="small" />
      ) : (
        <Tree
          showIcon
          treeData={getTreeData()}
          expandedKeys={expandedKeys}
          onExpand={handleExpand}
          onSelect={handleSelect}
          onRightClick={onRightClick}
          selectedKeys={currentCollection ? [`${currentDb}::${currentCollection}`] : currentDb ? [currentDb] : []}
          style={{ background: 'transparent' }}
        />
      )}

      <Modal
        title="Create Collection"
        open={createModalVisible}
        onOk={handleCreateCollection}
        onCancel={() => setCreateModalVisible(false)}
        okText="Create"
      >
        <Input
          placeholder="Collection name"
          value={newCollectionName}
          onChange={(e) => setNewCollectionName(e.target.value)}
          onPressEnter={handleCreateCollection}
        />
      </Modal>

      <Modal
        title="Drop Collection"
        open={deleteConfirmVisible}
        onOk={handleDeleteCollection}
        onCancel={() => setDeleteConfirmVisible(false)}
        okText="Drop"
        okButtonProps={{ danger: true }}
      >
        <p>Are you sure you want to drop <strong>{targetDeleteColl.coll}</strong>?</p>
        <p>This action cannot be undone.</p>
      </Modal>
    </Card>
  );
};

export default DatabaseTree;
