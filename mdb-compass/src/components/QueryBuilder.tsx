import React, { useState } from 'react';
import { Card, Input, Button, Space, Form, Select, InputNumber, Tag, message, Collapse, Divider, Typography } from 'antd';
import { PlusOutlined, DeleteOutlined, SearchOutlined } from '@ant-design/icons';
import { useQueryStore } from '../stores/queryStore';
import { useDatabaseStore } from '../stores/databaseStore';

const { Text } = Typography;
const { Option } = Select;

interface QueryField {
  id: string;
  field: string;
  operator: string;
  value: string;
}

interface SortField {
  id: string;
  field: string;
  direction: 1 | -1;
}

const QueryBuilder: React.FC = () => {
  const { filter, sort, projection, limit, skip, setFilter, setSort, setProjection, setLimit, setSkip, executeQuery, loading } = useQueryStore();
  const { currentDb, currentCollection } = useDatabaseStore();

  const [queryFields, setQueryFields] = useState<QueryField[]>([]);
  const [sortFields, setSortFields] = useState<SortField[]>([]);
  const [projectionFields, setProjectionFields] = useState<{ id: string; field: string; include: boolean }[]>([]);

  const operators = [
    { value: '$eq', label: 'Equal ($eq)' },
    { value: '$ne', label: 'Not Equal ($ne)' },
    { value: '$gt', label: 'Greater Than ($gt)' },
    { value: '$gte', label: 'Greater Than or Equal ($gte)' },
    { value: '$lt', label: 'Less Than ($lt)' },
    { value: '$lte', label: 'Less Than or Equal ($lte)' },
    { value: '$in', label: 'In ($in)' },
    { value: '$nin', label: 'Not In ($nin)' },
    { value: '$exists', label: 'Exists ($exists)' },
    { value: '$regex', label: 'Regex ($regex)' },
    { value: '$elemMatch', label: 'Element Match ($elemMatch)' },
  ];

  const addQueryField = () => {
    setQueryFields([...queryFields, { id: Date.now().toString(), field: '', operator: '$eq', value: '' }]);
  };

  const removeQueryField = (id: string) => {
    setQueryFields(queryFields.filter((f) => f.id !== id));
  };

  const updateQueryField = (id: string, updates: Partial<QueryField>) => {
    setQueryFields(queryFields.map((f) => (f.id === id ? { ...f, ...updates } : f)));
  };

  const addSortField = () => {
    setSortFields([...sortFields, { id: Date.now().toString(), field: '', direction: 1 }]);
  };

  const removeSortField = (id: string) => {
    setSortFields(sortFields.filter((f) => f.id !== id));
  };

  const updateSortField = (id: string, updates: Partial<SortField>) => {
    setSortFields(sortFields.map((f) => (f.id === id ? { ...f, ...updates } : f)));
  };

  const addProjectionField = () => {
    setProjectionFields([...projectionFields, { id: Date.now().toString(), field: '', include: true }]);
  };

  const removeProjectionField = (id: string) => {
    setProjectionFields(projectionFields.filter((f) => f.id !== id));
  };

  const updateProjectionField = (id: string, updates: Partial<{ field: string; include: boolean }>) => {
    setProjectionFields(projectionFields.map((f) => (f.id === id ? { ...f, ...updates } : f)));
  };

  const buildFilterString = (): string => {
    if (queryFields.length === 0) return '{}';

    const filterObj: Record<string, unknown> = {};
    for (const field of queryFields) {
      if (!field.field) continue;
      if (field.operator === '$exists') {
        filterObj[field.field] = { $exists: field.value === 'true' };
      } else if (field.operator === '$in' || field.operator === '$nin') {
        try {
          filterObj[field.field] = { [field.operator]: JSON.parse(field.value) };
        } catch {
          filterObj[field.field] = { [field.operator]: [field.value] };
        }
      } else if (field.operator === '$regex') {
        filterObj[field.field] = { $regex: field.value, $options: 'i' };
      } else if (field.operator === '$elemMatch') {
        try {
          filterObj[field.field] = { $elemMatch: JSON.parse(field.value) };
        } catch {
          filterObj[field.field] = { $elemMatch: {} };
        }
      } else {
        let parsedValue: unknown = field.value;
        try {
          parsedValue = JSON.parse(field.value);
        } catch {
          // keep as string
        }
        filterObj[field.field] = { [field.operator]: parsedValue };
      }
    }
    return JSON.stringify(filterObj);
  };

  const buildSortString = (): string => {
    if (sortFields.length === 0) return '{}';
    const sortObj: Record<string, 1 | -1> = {};
    for (const field of sortFields) {
      if (field.field) {
        sortObj[field.field] = field.direction;
      }
    }
    return JSON.stringify(sortObj);
  };

  const buildProjectionString = (): string => {
    if (projectionFields.length === 0) return '{}';
    const projObj: Record<string, 0 | 1> = {};
    for (const field of projectionFields) {
      if (field.field) {
        projObj[field.field] = field.include ? 1 : 0;
      }
    }
    return JSON.stringify(projObj);
  };

  const handleApply = () => {
    const filterStr = buildFilterString();
    const sortStr = buildSortString();
    const projStr = buildProjectionString();
    setFilter(filterStr);
    setSort(sortStr);
    setProjection(projStr);
  };

  const handleExecute = async () => {
    handleApply();
    if (!currentDb || !currentCollection) {
      message.warning('Select a database and collection first');
      return;
    }
    await executeQuery(currentDb, currentCollection);
  };

  return (
    <Card title="Query Builder" size="small" className="query-builder">
      <Collapse
        defaultActiveKey={['filter', 'sort', 'projection', 'options']}
        size="small"
      >
        <Collapse.Panel header="Filter" key="filter">
          {queryFields.map((field) => (
            <Space key={field.id} style={{ display: 'flex', marginBottom: 8 }} align="start">
              <Input
                placeholder="Field name"
                value={field.field}
                onChange={(e) => updateQueryField(field.id, { field: e.target.value })}
                style={{ width: 150 }}
              />
              <Select
                value={field.operator}
                onChange={(value) => updateQueryField(field.id, { operator: value })}
                style={{ width: 180 }}
              >
                {operators.map((op) => (
                  <Option key={op.value} value={op.value}>{op.label}</Option>
                ))}
              </Select>
              <Input
                placeholder="Value"
                value={field.value}
                onChange={(e) => updateQueryField(field.id, { value: e.target.value })}
                style={{ width: 200 }}
              />
              <Button type="text" danger icon={<DeleteOutlined />} onClick={() => removeQueryField(field.id)} />
            </Space>
          ))}
          <Button type="dashed" onClick={addQueryField} icon={<PlusOutlined />} block>
            Add Filter
          </Button>
          <Divider />
          <Text type="secondary" code>{buildFilterString()}</Text>
        </Collapse.Panel>

        <Collapse.Panel header="Sort" key="sort">
          {sortFields.map((field) => (
            <Space key={field.id} style={{ display: 'flex', marginBottom: 8 }} align="start">
              <Input
                placeholder="Field name"
                value={field.field}
                onChange={(e) => updateSortField(field.id, { field: e.target.value })}
                style={{ width: 200 }}
              />
              <Select
                value={field.direction}
                onChange={(value) => updateSortField(field.id, { direction: value })}
                style={{ width: 150 }}
              >
                <Option value={1}>Ascending (1)</Option>
                <Option value={-1}>Descending (-1)</Option>
              </Select>
              <Button type="text" danger icon={<DeleteOutlined />} onClick={() => removeSortField(field.id)} />
            </Space>
          ))}
          <Button type="dashed" onClick={addSortField} icon={<PlusOutlined />} block>
            Add Sort Field
          </Button>
        </Collapse.Panel>

        <Collapse.Panel header="Projection" key="projection">
          {projectionFields.map((field) => (
            <Space key={field.id} style={{ display: 'flex', marginBottom: 8 }} align="start">
              <Input
                placeholder="Field name"
                value={field.field}
                onChange={(e) => updateProjectionField(field.id, { field: e.target.value })}
                style={{ width: 200 }}
              />
              <Select
                value={field.include ? 1 : 0}
                onChange={(value) => updateProjectionField(field.id, { include: value === 1 })}
                style={{ width: 150 }}
              >
                <Option value={1}>Include (1)</Option>
                <Option value={0}>Exclude (0)</Option>
              </Select>
              <Button type="text" danger icon={<DeleteOutlined />} onClick={() => removeProjectionField(field.id)} />
            </Space>
          ))}
          <Button type="dashed" onClick={addProjectionField} icon={<PlusOutlined />} block>
            Add Projection Field
          </Button>
        </Collapse.Panel>

        <Collapse.Panel header="Options" key="options">
          <Space>
            <Form.Item label="Limit" style={{ marginBottom: 0 }}>
              <InputNumber
                min={0}
                max={100000}
                value={limit}
                onChange={(value) => setLimit(value || 20)}
              />
            </Form.Item>
            <Form.Item label="Skip" style={{ marginBottom: 0 }}>
              <InputNumber
                min={0}
                max={100000}
                value={skip}
                onChange={(value) => setSkip(value || 0)}
              />
            </Form.Item>
          </Space>
        </Collapse.Panel>
      </Collapse>

      <div style={{ marginTop: 12 }}>
        <Space>
          <Button onClick={handleApply}>Apply Query</Button>
          <Button type="primary" icon={<SearchOutlined />} onClick={handleExecute} loading={loading}>
            Find
          </Button>
        </Space>
      </div>
    </Card>
  );
};

export default QueryBuilder;
