import { create } from 'zustand';

interface CollectionInfo {
  name: string;
  type: string;
  options: Record<string, unknown>;
}

interface IndexInfo {
  name: string;
  key: Record<string, unknown>;
  unique?: boolean;
  sparse?: boolean;
  expireAfterSeconds?: number;
}

interface DatabaseState {
  currentDb: string | null;
  currentCollection: string | null;
  collections: CollectionInfo[];
  indexes: IndexInfo[];
  collectionsLoading: boolean;
  indexesLoading: boolean;
  selectDatabase: (dbName: string) => Promise<void>;
  selectCollection: (dbName: string, collectionName: string) => Promise<void>;
  refreshCollections: (dbName: string) => Promise<void>;
  refreshIndexes: (dbName: string, collectionName: string) => Promise<void>;
  createCollection: (dbName: string, collectionName: string) => Promise<boolean>;
  dropCollection: (dbName: string, collectionName: string) => Promise<boolean>;
}

export const useDatabaseStore = create<DatabaseState>((set) => ({
  currentDb: null,
  currentCollection: null,
  collections: [],
  indexes: [],
  collectionsLoading: false,
  indexesLoading: false,

  selectDatabase: async (dbName: string) => {
    set({ currentDb: dbName, currentCollection: null, indexes: [] });
    await useDatabaseStore.getState().refreshCollections(dbName);
  },

  selectCollection: async (dbName: string, collectionName: string) => {
    set({ currentDb: dbName, currentCollection: collectionName });
    await useDatabaseStore.getState().refreshIndexes(dbName, collectionName);
  },

  refreshCollections: async (dbName: string) => {
    set({ collectionsLoading: true });
    try {
      const result = await window.mdb.listCollections(dbName);
      if (result.collections) {
        set({ collections: result.collections, collectionsLoading: false });
      } else {
        set({ collectionsLoading: false });
      }
    } catch {
      set({ collectionsLoading: false });
    }
  },

  refreshIndexes: async (dbName: string, collectionName: string) => {
    set({ indexesLoading: true });
    try {
      const result = await window.mdb.listIndexes(dbName, collectionName);
      if (result.indexes) {
        set({ indexes: result.indexes, indexesLoading: false });
      } else {
        set({ indexesLoading: false });
      }
    } catch {
      set({ indexesLoading: false });
    }
  },

  createCollection: async (dbName: string, collectionName: string) => {
    try {
      const result = await window.mdb.createCollection(dbName, collectionName);
      if (result.success) {
        await useDatabaseStore.getState().refreshCollections(dbName);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  },

  dropCollection: async (dbName: string, collectionName: string) => {
    try {
      const result = await window.mdb.dropCollection(dbName, collectionName);
      if (result.success) {
        set({ currentCollection: null, indexes: [] });
        await useDatabaseStore.getState().refreshCollections(dbName);
        return true;
      }
      return false;
    } catch {
      return false;
    }
  },
}));
