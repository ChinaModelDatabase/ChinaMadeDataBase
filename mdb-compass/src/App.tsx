import React from 'react';
import { Layout, Tabs, Typography } from 'antd';
import {
  DatabaseOutlined,
  SearchOutlined,
  FileTextOutlined,
  BarChartOutlined,
  ClusterOutlined,
  CloudServerOutlined,
  ApartmentOutlined,
  UploadOutlined,
} from '@ant-design/icons';
import ConnectionPanel from './components/ConnectionPanel';
import DatabaseTree from './components/DatabaseTree';
import DocumentViewer from './components/DocumentViewer';
import QueryBuilder from './components/QueryBuilder';
import IndexManager from './components/IndexManager';
import AggregationBuilder from './components/AggregationBuilder';
import PerformanceCharts from './components/PerformanceCharts';
import ReplicaSetStatus from './components/ReplicaSetStatus';
import ShardingStatus from './components/ShardingStatus';
import GridFSManager from './components/GridFSManager';
import { useConnectionStore } from './stores/connectionStore';
import { useDatabaseStore } from './stores/databaseStore';

const { Header, Sider, Content } = Layout;
const { Title } = Typography;

const App: React.FC = () => {
  const { connected, databases } = useConnectionStore();
  const { currentDb, currentCollection } = useDatabaseStore();

  const getTabItems = () => {
    const items = [
      {
        key: 'documents',
        label: (
          <span>
            <FileTextOutlined />
            Documents
          </span>
        ),
        children: <DocumentViewer />,
      },
      {
        key: 'query',
        label: (
          <span>
            <SearchOutlined />
            Query Builder
          </span>
        ),
        children: <QueryBuilder />,
      },
      {
        key: 'indexes',
        label: (
          <span>
            <DatabaseOutlined />
            Indexes
          </span>
        ),
        children: <IndexManager />,
      },
      {
        key: 'aggregation',
        label: (
          <span>
            <ApartmentOutlined />
            Aggregation
          </span>
        ),
        children: <AggregationBuilder />,
      },
      {
        key: 'performance',
        label: (
          <span>
            <BarChartOutlined />
            Performance
          </span>
        ),
        children: <PerformanceCharts />,
      },
      {
        key: 'replica',
        label: (
          <span>
            <ClusterOutlined />
            Replica Set
          </span>
        ),
        children: <ReplicaSetStatus />,
      },
      {
        key: 'sharding',
        label: (
          <span>
            <CloudServerOutlined />
            Sharding
          </span>
        ),
        children: <ShardingStatus />,
      },
      {
        key: 'gridfs',
        label: (
          <span>
            <UploadOutlined />
            GridFS
          </span>
        ),
        children: <GridFSManager />,
      },
    ];

    if (!connected) {
      return items.filter((item) =>
        ['performance'].includes(item.key)
      );
    }

    if (!currentDb || !currentCollection) {
      return items.filter((item) =>
        ['performance', 'replica', 'sharding'].includes(item.key)
      );
    }

    return items;
  };

  return (
    <Layout className="app-layout" style={{ minHeight: '100vh' }}>
      <Header className="app-header">
        <div className="header-content">
          <div className="header-left">
            <DatabaseOutlined style={{ fontSize: 24, color: '#13c2c2' }} />
            <Title level={4} style={{ margin: 0, color: '#fff' }}>
              MDB Compass
            </Title>
          </div>
          <div className="header-right">
            {connected && (
              <span className="connection-info">
                {currentDb && <span className="db-name">{currentDb}</span>}
                {currentDb && currentCollection && (
                  <>
                    <span className="separator">.</span>
                    <span className="coll-name">{currentCollection}</span>
                  </>
                )}
              </span>
            )}
          </div>
        </div>
      </Header>

      <Layout>
        <Sider width={300} className="app-sider" theme="dark">
          <div className="sider-content">
            <ConnectionPanel />
            <div style={{ marginTop: 12 }}>
              <DatabaseTree />
            </div>
            {connected && (
              <div style={{ marginTop: 12 }}>
                <div className="stats-card">
                  <div className="stat-item">
                    <span className="stat-label">Databases</span>
                    <span className="stat-value">{databases.length}</span>
                  </div>
                </div>
              </div>
            )}
          </div>
        </Sider>

        <Content className="app-content">
          <Tabs
            items={getTabItems()}
            defaultActiveKey="documents"
            className="main-tabs"
            destroyInactiveTabPane
          />
        </Content>
      </Layout>
    </Layout>
  );
};

export default App;
