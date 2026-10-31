import { create } from 'zustand';

interface ConnectionState {
  host: string;
  port: number;
  username: string;
  password: string;
  authSource: string;
  tls: boolean;
  connected: boolean;
  connecting: boolean;
  error: string | null;
  databases: Array<{ name: string; sizeOnDisk: number; empty: boolean }>;
  setHost: (host: string) => void;
  setPort: (port: number) => void;
  setUsername: (username: string) => void;
  setPassword: (password: string) => void;
  setAuthSource: (authSource: string) => void;
  setTls: (tls: boolean) => void;
  connect: () => Promise<void>;
  disconnect: () => Promise<void>;
  refreshDatabases: () => Promise<void>;
}

export const useConnectionStore = create<ConnectionState>((set, get) => ({
  host: 'localhost',
  port: 27017,
  username: '',
  password: '',
  authSource: 'admin',
  tls: false,
  connected: false,
  connecting: false,
  error: null,
  databases: [],

  setHost: (host) => set({ host }),
  setPort: (port) => set({ port }),
  setUsername: (username) => set({ username }),
  setPassword: (password) => set({ password }),
  setAuthSource: (authSource) => set({ authSource }),
  setTls: (tls) => set({ tls }),

  connect: async () => {
    const { host, port, username, password, authSource, tls } = get();
    set({ connecting: true, error: null });
    try {
      const result = await window.mdb.connect({ host, port, username, password, authSource, tls });
      if (result.success) {
        set({ connected: true, connecting: false, error: null });
        await get().refreshDatabases();
      } else {
        set({ connected: false, connecting: false, error: result.error || 'Connection failed' });
      }
    } catch (err) {
      set({ connected: false, connecting: false, error: String(err) });
    }
  },

  disconnect: async () => {
    try {
      await window.mdb.disconnect();
      set({ connected: false, databases: [], error: null });
    } catch (err) {
      set({ error: String(err) });
    }
  },

  refreshDatabases: async () => {
    try {
      const result = await window.mdb.listDatabases();
      if (result.databases) {
        set({ databases: result.databases });
      }
    } catch (err) {
      set({ error: String(err) });
    }
  },
}));
