'use client';

import { WorkflowNode, ModelInfo, fetchModels, updateAPIKey } from '@/lib/api';
import { useState, useEffect } from 'react';
import toast from 'react-hot-toast';

interface ConfigPanelProps {
  selectedNode: WorkflowNode | null;
  models: ModelInfo[];
  onModelsChange: () => void;
}

export default function ConfigPanel({ selectedNode, models, onModelsChange }: ConfigPanelProps) {
  return (
    <div className="w-80 border-l border-gray-200 bg-white overflow-y-auto p-4">
      <h2 className="text-lg font-bold mb-4">
        {selectedNode ? `Edit: ${selectedNode.data.nodeName || selectedNode.data.label}` : 'Settings'}
      </h2>

      {selectedNode ? (
        <NodeEditor node={selectedNode} models={models} />
      ) : (
        <ModelsConfig models={models} onReload={onModelsChange} />
      )}
    </div>
  );
}

function NodeEditor({ node, models }: { node: WorkflowNode; models: ModelInfo[] }) {
  const [data, setData] = useState({ ...node.data });

  useEffect(() => {
    setData({ ...node.data });
  }, [node]);

  return (
    <div className="space-y-3">
      <div>
        <label className="block text-sm font-medium text-gray-700 mb-1">Node Name</label>
        <input
          className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
          value={data.nodeName}
          onChange={(e) => setData({ ...data, nodeName: e.target.value })}
        />
      </div>

      {node.type === 'agent' && (
        <>
          {models.length > 0 && (
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Model</label>
              <select
                className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
                value={data.modelName || ''}
                onChange={(e) => setData({ ...data, modelName: e.target.value })}
              >
                <option value="">Default model</option>
                {models.map((m) => (
                  <option key={m.name} value={m.name} disabled={!m.hasApiKey}>
                    {m.displayName} {!m.hasApiKey ? '(no API key)' : ''}
                  </option>
                ))}
              </select>
            </div>
          )}

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">System Prompt</label>
            <textarea
              className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm h-24 font-mono"
              value={data.systemPrompt || data.instructions || ''}
              onChange={(e) => setData({ ...data, systemPrompt: e.target.value })}
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gray-700 mb-1">Temperature</label>
            <input
              type="number"
              min="0"
              max="2"
              step="0.1"
              className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm"
              value={data.temperature ?? ''}
              placeholder="Use default"
              onChange={(e) => setData({ ...data, temperature: parseFloat(e.target.value) || 0 })}
            />
          </div>
        </>
      )}

      {node.type === 'if-else' && (
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Condition</label>
          <textarea
            className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm h-20 font-mono"
            value={data.condition || ''}
            onChange={(e) => setData({ ...data, condition: e.target.value })}
          />
          <p className="text-xs text-gray-400 mt-1">
            True branch: connect left handle. False branch: connect right handle.
          </p>
        </div>
      )}

      {node.type === 'transform' && (
        <div>
          <label className="block text-sm font-medium text-gray-700 mb-1">Transform Code</label>
          <textarea
            className="w-full border border-gray-300 rounded px-2 py-1.5 text-sm h-24 font-mono"
            value={data.transformCode || ''}
            placeholder="Use {{variableName}} or {{outputs.nodeId}}"
            onChange={(e) => setData({ ...data, transformCode: e.target.value })}
          />
        </div>
      )}
    </div>
  );
}

function ModelsConfig({ models, onReload }: { models: ModelInfo[]; onReload: () => void }) {
  const [apiKeys, setApiKeys] = useState<Record<string, string>>({});
  const [saving, setSaving] = useState<Record<string, boolean>>({});

  const handleSaveKey = async (name: string) => {
    const key = apiKeys[name];
    if (!key) {
      toast.error('Enter an API key first');
      return;
    }
    setSaving((prev) => ({ ...prev, [name]: true }));
    try {
      await updateAPIKey(name, key);
      toast.success(`API key saved for ${name}`);
      onReload();
    } catch (err: any) {
      toast.error(err.message || 'Failed to save API key');
    } finally {
      setSaving((prev) => ({ ...prev, [name]: false }));
    }
  };

  return (
    <div className="space-y-4">
      <p className="text-sm text-gray-500">Configure API keys for each model.</p>

      {models.length === 0 && (
        <div className="text-sm text-gray-400 italic">No models configured in backend.</div>
      )}

      {models.map((m) => (
        <div key={m.name} className="border border-gray-200 rounded-lg p-3">
          <div className="flex items-center justify-between mb-1">
            <span className="text-sm font-medium">{m.displayName}</span>
            <span
              className={`text-xs px-1.5 py-0.5 rounded ${
                m.hasApiKey ? 'bg-green-100 text-green-700' : 'bg-red-100 text-red-700'
              }`}
            >
              {m.hasApiKey ? 'Key set' : 'No key'}
            </span>
          </div>
          <div className="text-xs text-gray-400 mb-2">
            {m.model} &middot; {m.apiBase}
          </div>

          {!m.hasApiKey && (
            <div className="flex gap-2">
              <input
                type="password"
                className="flex-1 border border-gray-300 rounded px-2 py-1 text-sm"
                placeholder={`API key for ${m.name}`}
                value={apiKeys[m.name] || ''}
                onChange={(e) => setApiKeys({ ...apiKeys, [m.name]: e.target.value })}
              />
              <button
                className="px-3 py-1 bg-blue-600 text-white rounded text-sm hover:bg-blue-700 disabled:opacity-50"
                disabled={saving[m.name]}
                onClick={() => handleSaveKey(m.name)}
              >
                {saving[m.name] ? '...' : 'Save'}
              </button>
            </div>
          )}
        </div>
      ))}
    </div>
  );
}
