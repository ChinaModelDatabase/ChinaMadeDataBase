import React, { useState, useEffect, useCallback } from 'react';
import { Card, Table, Button, Space, Upload, Modal, Input, Tag, message, Empty, Popconfirm } from 'antd';
import { UploadOutlined, DownloadOutlined, DeleteOutlined, ReloadOutlined, InboxOutlined } from '@ant-design/icons';
import { useConnectionStore } from '../stores/connectionStore';
import { useDatabaseStore } from '../stores/databaseStore';
import { mdbService } from '../services/mdbService';

const { Dragger } = Upload;

interface GridFSFile {
  _id: string;
  filename: string;
  length: number;
  chunkSize: number;
  uploadDate: string;
  metadata?: Record<string, unknown>;
}

const GridFSManager: React.FC = () => {
  const { connected } = useConnectionStore();
  const { currentDb } = useDatabaseStore();
  const [files, setFiles] = useState<GridFSFile[]>([]);
  const [loading, setLoading] = useState(false);
  const [bucketName, setBucketName] = useState('fs');
  const [uploading, setUploading] = useState(false);

  const fetchFiles = useCallback(async () => {
    if (!connected || !currentDb) return;
    setLoading(true);
    const result = await mdbService.gridfsListFiles(currentDb, bucketName);
    if (result.files) {
      setFiles(result.files);
    }
    setLoading(false);
  }, [connected, currentDb, bucketName]);

  useEffect(() => {
    fetchFiles();
  }, [fetchFiles]);

  const handleUpload = async (file: File) => {
    if (!currentDb) return;
    setUploading(true);
    const arrayBuffer = await file.arrayBuffer();
    const result = await mdbService.gridfsUpload(currentDb, bucketName, file.name, arrayBuffer);
    setUploading(false);

    if (result.error) {
      message.error(result.error);
    } else {
      message.success(`File "${file.name}" uploaded`);
      fetchFiles();
    }
    return false;
  };

  const handleDownload = async (file: GridFSFile) => {
    if (!currentDb) return;
    const result = await mdbService.gridfsDownload(currentDb, bucketName, file._id);
    if (result.error) {
      message.error(result.error);
    } else if (result.data) {
      const buffer = new Uint8Array(result.data);
      const blob = new Blob([buffer]);
      const url = URL.createObjectURL(blob);
      const a = document.createElement('a');
      a.href = url;
      a.download = file.filename;
      a.click();
      URL.revokeObjectURL(url);
      message.success(`Downloaded "${file.filename}"`);
    }
  };

  const handleDelete = async (file: GridFSFile) => {
    if (!currentDb) return;
    const result = await mdbService.gridfsDelete(currentDb, bucketName, file._id);
    if (result.error) {
      message.error(result.error);
    } else {
      message.success(`File "${file.filename}" deleted`);
      fetchFiles();
    }
  };

  const formatBytes = (bytes: number): string => {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
  };

  const columns = [
    {
      title: 'Filename',
      dataIndex: 'filename',
      key: 'filename',
      render: (name: string) => <code>{name}</code>,
    },
    {
      title: 'Size',
      dataIndex: 'length',
      key: 'length',
      width: 120,
      render: (length: number) => formatBytes(length),
    },
    {
      title: 'Chunk Size',
      dataIndex: 'chunkSize',
      key: 'chunkSize',
      width: 120,
      render: (size: number) => formatBytes(size),
    },
    {
      title: 'Upload Date',
      dataIndex: 'uploadDate',
      key: 'uploadDate',
      width: 180,
      render: (date: string) => new Date(date).toLocaleString(),
    },
    {
      title: 'Actions',
      key: 'actions',
      width: 120,
      render: (_: unknown, record: GridFSFile) => (
        <Space>
          <Button type="text" size="small" icon={<DownloadOutlined />} onClick={() => handleDownload(record)} />
          <Popconfirm
            title="Delete this file?"
            onConfirm={() => handleDelete(record)}
            okText="Delete"
            okButtonProps={{ danger: true }}
          >
            <Button type="text" danger size="small" icon={<DeleteOutlined />} />
          </Popconfirm>
        </Space>
      ),
    },
  ];

  if (!connected || !currentDb) {
    return (
      <Card title="GridFS Manager" size="small">
        <Empty description="Connect to a database and select a database first" />
      </Card>
    );
  }

  return (
    <Card
      title="GridFS Manager"
      size="small"
      extra={
        <Space>
          <Input
            size="small"
            value={bucketName}
            onChange={(e) => setBucketName(e.target.value)}
            addonBefore="Bucket"
            style={{ width: 200 }}
          />
          <Button size="small" icon={<ReloadOutlined />} onClick={fetchFiles} loading={loading}>
            Refresh
          </Button>
        </Space>
      }
    >
      <Dragger
        customRequest={({ file }) => handleUpload(file as File)}
        showUploadList={false}
        disabled={uploading}
        style={{ marginBottom: 16 }}
      >
        <p className="ant-upload-drag-icon">
          <InboxOutlined />
        </p>
        <p className="ant-upload-text">Click or drag file to upload to GridFS</p>
        <p className="ant-upload-hint">Supports any file type</p>
      </Dragger>

      <Table
        columns={columns}
        dataSource={files}
        rowKey="_id"
        loading={loading}
        size="small"
        pagination={false}
        locale={{ emptyText: 'No files in GridFS bucket' }}
      />
    </Card>
  );
};

export default GridFSManager;
