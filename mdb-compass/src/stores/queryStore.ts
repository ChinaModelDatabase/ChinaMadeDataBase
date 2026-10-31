import { create } from 'zustand';

interface QueryState {
  filter: string;
  sort: string;
  projection: string;
  limit: number;
  skip: number;
  results: Record<string, unknown>[];
  total: number;
  loading: boolean;
  error: string | null;
  activeTab: string;
  setFilter: (filter: string) => void;
  setSort: (sort: string) => void;
  setProjection: (projection: string) => void;
  setLimit: (limit: number) => void;
  setSkip: (skip: number) => void;
  setActiveTab: (tab: string) => void;
  executeQuery: (dbName: string, collectionName: string) => Promise<void>;
  clearResults: () => void;
}

export const useQueryStore = create<QueryState>((set, get) => ({
  filter: '{}',
  sort: '{}',
  projection: '{}',
  limit: 20,
  skip: 0,
  results: [],
  total: 0,
  loading: false,
  error: null,
  activeTab: 'documents',

  setFilter: (filter) => set({ filter }),
  setSort: (sort) => set({ sort }),
  setProjection: (projection) => set({ projection }),
  setLimit: (limit) => set({ limit }),
  setSkip: (skip) => set({ skip }),
  setActiveTab: (tab) => set({ activeTab: tab }),

  executeQuery: async (dbName: string, collectionName: string) => {
    const { filter, sort, projection, limit, skip } = get();
    set({ loading: true, error: null });
    try {
      const parsedSort = sort && sort !== '{}' ? JSON.parse(sort) : undefined;
      const parsedProjection = projection && projection !== '{}' ? JSON.parse(projection) : undefined;

      const result = await window.mdb.find(dbName, collectionName, filter, {
        sort: parsedSort,
        projection: parsedProjection,
        limit,
        skip,
      });

      if (result.error) {
        set({ error: result.error, loading: false });
      } else {
        set({
          results: result.documents || [],
          total: result.total || 0,
          loading: false,
        });
      }
    } catch (err) {
      set({ error: String(err), loading: false });
    }
  },

  clearResults: () => set({ results: [], total: 0, error: null }),
}));
