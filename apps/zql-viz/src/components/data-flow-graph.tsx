import type {FC} from 'react';
import {useMemo} from 'react';
import {
  ReactFlow,
  Background,
  Controls,
  type Node,
  type Edge,
  MarkerType,
  Handle,
  Position,
  useNodesState,
  useEdgesState,
  ReactFlowProvider,
} from '@xyflow/react';
import '@xyflow/react/dist/style.css';
import type {Graph} from '../types.ts';

interface DataFlowGraphProps {
  graph: Graph;
}

// Node styles based on type
const getNodeStyle = (nodeType: string) => {
  const baseStyle = {
    padding: '10px 15px',
    borderRadius: '8px',
    fontSize: '12px',
    fontWeight: '500',
    minWidth: '80px',
    textAlign: 'center' as const,
    border: '2px solid',
  };

  switch (nodeType.toLowerCase()) {
    case 'table':
      return {
        ...baseStyle,
        backgroundColor: '#4F46E5',
        color: 'white',
        borderColor: '#3730A3',
      };
    case 'join':
      return {
        ...baseStyle,
        backgroundColor: '#059669',
        color: 'white',
        borderColor: '#047857',
      };
    case 'filter':
    case 'where':
      return {
        ...baseStyle,
        backgroundColor: '#DC2626',
        color: 'white',
        borderColor: '#B91C1C',
      };
    case 'sort':
    case 'orderby':
      return {
        ...baseStyle,
        backgroundColor: '#7C2D12',
        color: 'white',
        borderColor: '#92400E',
      };
    case 'limit':
      return {
        ...baseStyle,
        backgroundColor: '#7C3AED',
        color: 'white',
        borderColor: '#6D28D9',
      };
    case 'exists':
      return {
        ...baseStyle,
        backgroundColor: '#EA580C',
        color: 'white',
        borderColor: '#C2410C',
      };
    case 'select':
    case 'projection':
      return {
        ...baseStyle,
        backgroundColor: '#0891B2',
        color: 'white',
        borderColor: '#0E7490',
      };
    default:
      return {
        ...baseStyle,
        backgroundColor: '#6B7280',
        color: 'white',
        borderColor: '#4B5563',
      };
  }
};

// Custom node component
const CustomNode: FC<{data: Graph['nodes'][number]}> = ({data}) => {
  const style = getNodeStyle(data.type);

  return (
    <div style={{position: 'relative'}}>
      <Handle
        type="target"
        position={Position.Top}
        style={{
          background: '#555',
          width: 8,
          height: 8,
        }}
      />
      <div style={style}>
        <div style={{fontWeight: 'bold', marginBottom: '4px'}}>{data.type}</div>
        <div style={{fontSize: '11px', opacity: 0.9}}>{data.name}</div>
      </div>
      <Handle
        type="source"
        position={Position.Bottom}
        style={{
          background: '#555',
          width: 8,
          height: 8,
        }}
      />
    </div>
  );
};

const nodeTypes = {
  custom: CustomNode,
};

// Calculate hierarchical layout
const calculateLayout = (nodes: Graph['nodes'], edges: Graph['edges']) => {
  // Find root nodes (no incoming edges)
  const incomingEdges = new Set(edges.map(e => e.dest));
  const rootNodes = nodes.filter(node => !incomingEdges.has(node.id));

  // Build adjacency list
  const adjList = new Map<number, number[]>();
  edges.forEach(edge => {
    if (!adjList.has(edge.source)) {
      adjList.set(edge.source, []);
    }
    adjList.get(edge.source)!.push(edge.dest);
  });

  // Calculate levels using BFS
  const levels = new Map<number, number>();
  const queue: Array<{nodeId: number; level: number}> = [];

  // Start with root nodes at level 0
  rootNodes.forEach(node => {
    levels.set(node.id, 0);
    queue.push({nodeId: node.id, level: 0});
  });

  while (queue.length > 0) {
    const {nodeId, level} = queue.shift()!;
    const children = adjList.get(nodeId) || [];

    children.forEach(childId => {
      const currentLevel = levels.get(childId) ?? -1;
      const newLevel = level + 1;

      if (newLevel > currentLevel) {
        levels.set(childId, newLevel);
        queue.push({nodeId: childId, level: newLevel});
      }
    });
  }

  // Group nodes by level
  const nodesByLevel = new Map<number, number[]>();
  levels.forEach((level, nodeId) => {
    if (!nodesByLevel.has(level)) {
      nodesByLevel.set(level, []);
    }
    nodesByLevel.get(level)!.push(nodeId);
  });

  // Position nodes
  const layoutNodes: Node[] = [];
  const levelHeight = 150;
  const nodeWidth = 200;

  nodesByLevel.forEach((nodeIds, level) => {
    const y = level * levelHeight + 50;
    const totalWidth = nodeIds.length * nodeWidth;
    const startX = -totalWidth / 2;

    nodeIds.forEach((nodeId, index) => {
      const node = nodes.find(n => n.id === nodeId)!;
      const x = startX + index * nodeWidth + nodeWidth / 2;

      layoutNodes.push({
        id: nodeId.toString(),
        type: 'custom',
        position: {x, y},
        data: {
          name: node.name,
          type: node.type,
        },
      });
    });
  });

  return layoutNodes;
};

const DataFlowGraphInner: FC<DataFlowGraphProps> = ({graph}) => {
  const layoutNodes = useMemo(() => {
    if (!graph.nodes.length) return [];
    return calculateLayout(graph.nodes, graph.edges);
  }, [graph]);

  const flowEdges = useMemo(() => {
    return graph.edges.map(
      edge =>
        ({
          id: `${edge.source}-${edge.dest}`,
          source: edge.source.toString(),
          target: edge.dest.toString(),
          type: 'smoothstep',
          animated: false,
          style: {stroke: '#64748B', strokeWidth: 2},
          markerEnd: {
            type: MarkerType.ArrowClosed,
            color: '#64748B',
          },
        }) as Edge,
    );
  }, [graph.edges]);

  const [nodes, , onNodesChange] = useNodesState(layoutNodes);
  const [edges, , onEdgesChange] = useEdgesState(flowEdges);

  if (!graph.nodes.length) {
    return (
      <div
        style={{
          height: '100%',
          display: 'flex',
          alignItems: 'center',
          justifyContent: 'center',
          color: '#6B7280',
        }}
      >
        No graph data available
      </div>
    );
  }

  return (
    <div style={{height: '100%', width: '100%'}}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        onNodesChange={onNodesChange}
        onEdgesChange={onEdgesChange}
        nodeTypes={nodeTypes}
        fitView
        fitViewOptions={{padding: 0.2}}
        minZoom={0.1}
        maxZoom={2}
      >
        <Background color="#374151" gap={20} />
        <Controls />
      </ReactFlow>
    </div>
  );
};

export const DataFlowGraph: FC<DataFlowGraphProps> = ({graph}) => {
  return (
    <ReactFlowProvider>
      <DataFlowGraphInner graph={graph} />
    </ReactFlowProvider>
  );
};
