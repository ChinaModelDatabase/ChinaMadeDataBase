import React, { useState } from 'react';
import { Card, Button, Space, Select, Input, Form, Tag, message, Empty, Collapse, Typography, Divider } from 'antd';
import { PlusOutlined, DeleteOutlined, PlayCircleOutlined, ArrowUpOutlined, ArrowDownOutlined } from '@ant-design/icons';
import { useDatabaseStore } from '../stores/databaseStore';
import { mdbService } from '../services/mdbService';

const { Option } = Select;
const { Text } = Typography;

interface PipelineStage {
  id: string;
  type: string;
  config: string;
  enabled: boolean;
}

const STAGE_TYPES = [
  { value: '$match', label: '$match', description: 'Filter documents' },
  { value: '$group', label: '$group', description: 'Group documents' },
  { value: '$sort', label: '$sort', description: 'Sort documents' },
  { value: '$limit', label: '$limit', description: 'Limit documents' },
  { value: '$skip', label: '$skip', description: 'Skip documents' },
  { value: '$project', label: '$project', description: 'Reshape documents' },
  { value: '$unwind', label: '$unwind', description: 'Deconstruct array field' },
  { value: '$addFields', label: '$addFields', description: 'Add new fields' },
  { value: '$count', label: '$count', description: 'Count documents' },
  { value: '$lookup', label: '$lookup', description: 'Join collections' },
  { value: '$sample', label: '$sample', description: 'Random sample' },
  { value: '$facet', label: '$facet', description: 'Multiple pipelines' },
];

const DEFAULT_CONFIGS: Record<string, string> = {
  '$match': '{ "field": "value" }',
  '$group': '{ "_id": "$field", "count": { "$sum": 1 } }',
  '$sort': '{ "field": 1 }',
  '$limit': '10',
  '$skip': '0',
  '$project': '{ "field": 1, "_id": 0 }',
  '$unwind': "$arrayField",
  '$addFields': '{ "newField": { "$literal": "value" } }',
  '$count': "total",
  '$lookup': '{ "from": "otherCollection", "localField": "_id", "foreignField": "field", "as": "result" }',
  '$sample': '{ "size": 5 }',
  '$facet': '{ "output1": [{ "$match": {} }], "output2": [{ "$count": "total" }] }',
};

const AggregationBuilder: React.FC = () => {
  const { currentDb, currentCollection } = useDatabaseStore();
  const [stages, setStages] = useState<PipelineStage[]>([]);
  const [results, setResults] = useState<Record<string, unknown>[]>([]);
  const [loading, setLoading] = useState(false);

  const addStage = (type: string) => {
    const newStage: PipelineStage = {
      id: Date.now().toString(),
      type,
      config: DEFAULT_CONFIGS[type] || '{}',
      enabled: true,
    };
    setStages([...stages, newStage]);
  };

  const removeStage = (id: string) => {
    setStages(stages.filter((s) => s.id !== id));
  };

  const toggleStage = (id: string) => {
    setStages(stages.map((s) => (s.id === id ? { ...s, enabled: !s.enabled } : s)));
  };

  const updateStageConfig = (id: string, config: string) => {
    setStages(stages.map((s) => (s.id === id ? { ...s, config } : s)));
  };

  const moveStage = (id: string, direction: 'up' | 'down') => {
    const idx = stages.findIndex((s) => s.id === id);
    if (idx === -1) return;
    const newStages = [...stages];
    const swapIdx = direction === 'up' ? idx - 1 : idx + 1;
    if (swapIdx < 0 || swapIdx >= newStages.length) return;
    [newStages[idx], newStages[swapIdx]] = [newStages[swapIdx], newStages[idx]];
    setStages(newStages);
  };

  const buildPipeline = (): Record<string, unknown>[] => {
    const pipeline: Record<string, unknown>[] = [];
    for (const stage of stages) {
      if (!stage.enabled) continue;
      try {
        const config = JSON.parse(stage.config);
        const stageDoc: Record<string, unknown> = {};
        stageDoc[stage.type] = config;
        pipeline.push(stageDoc);
      } catch {
        message.error(`Invalid JSON in ${stage.type} stage`);
        return [];
      }
    }
    return pipeline;
  };

  const handleExecute = async () => {
    if (!currentDb || !currentCollection) {
      message.warning('Select a database and collection first');
      return;
    }

    const pipeline = buildPipeline();
    if (pipeline.length === 0) {
      message.warning('Add at least one enabled stage');
      return;
    }

    setLoading(true);
    const result = await mdbService.aggregate(currentDb, currentCollection, pipeline);
    setLoading(false);

    if (result.error) {
      message.error(result.error);
    } else {
      setResults(result.results || []);
      message.success(`Pipeline returned ${result.results?.length || 0} documents`);
    }
  };

  return (
    <Card title="Aggregation Pipeline" size="small" className="aggregation-builder">
      <div style={{ display: 'flex', gap: 16 }}>
        <div style={{ flex: 1 }}>
          <div style={{ marginBottom: 8 }}>
            <Select
              placeholder="Add stage..."
              style={{ width: '100%' }}
              onChange={(value) => addStage(value)}
              value={undefined}
            >
              {STAGE_TYPES.map((stage) => (
                <Option key={stage.value} value={stage.value}>
                  <Space>
                    <Tag>{stage.value}</Tag>
                    <Text type="secondary">{stage.description}</Text>
                  </Space>
                </Option>
              ))}
            </Select>
          </div>

          {stages.length === 0 ? (
            <Empty description="No pipeline stages. Select a stage type above." />
          ) : (
            <Collapse size="small" defaultActiveKey={stages.map((s) => s.id)}>
              {stages.map((stage, idx) => (
                <Collapse.Panel
                  key={stage.id}
                  header={
                    <Space>
                      <Tag color={stage.enabled ? 'blue' : 'default'}>{idx + 1}</Tag>
                      <Tag>{stage.type}</Tag>
                      {!stage.enabled && <Tag color="red">Disabled</Tag>}
                    </Space>
                  }
                  extra={
                    <Space onClick={(e) => e.stopPropagation()}>
                      <Button size="small" type="text" icon={<ArrowUpOutlined />} onClick={() => moveStage(stage.id, 'up')} disabled={idx === 0} />
                      <Button size="small" type="text" icon={<ArrowUpOutlined style={{ transform: 'rotate(180deg)' }} />} onClick={() => moveStage(stage.id, 'down')} disabled={idx === stages.length - 1} />
                      <Button size="small" type="text" onClick={() => toggleStage(stage.id)}>
                        {stage.enabled ? 'Disable' : 'Enable'}
                      </Button>
                      <Button size="small" type="text" danger icon={<DeleteOutlined />} onClick={() => removeStage(stage.id)} />
                    </Space>
                  }
                >
                  <Input.TextArea
                    value={stage.config}
                    onChange={(e) => updateStageConfig(stage.id, e.target.value)}
                    autoSize={{ minRows: 2, maxRows: 8 }}
                    style={{ fontFamily: 'monospace' }}
                  />
                </Collapse.Panel>
              ))}
            </Collapse>
          )}

          <div style={{ marginTop: 12 }}>
            <Button type="primary" icon={<PlayCircleOutlined />} onClick={handleExecute} loading={loading} block>
              Execute Pipeline
            </Button>
          </div>
        </div>

        <div style={{ flex: 1 }}>
          <Card title={`Results (${results.length})`} size="small" style={{ height: '100%' }}>
            {results.length > 0 ? (
              <div style={{ maxHeight: 500, overflow: 'auto' }}>
                <pre style={{ fontSize: 11, margin: 0 }}>
                  {JSON.stringify(results, null, 2)}
                </pre>
              </div>
            ) : (
              <Empty description="No results. Execute the pipeline to see output." />
            )}
          </Card>
        </div>
      </div>
    </Card>
  );
};

export default AggregationBuilder;
