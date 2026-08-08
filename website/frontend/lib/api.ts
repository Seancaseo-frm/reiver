import axios from 'axios';

const API_URL = process.env.NEXT_PUBLIC_API_URL || (typeof window !== 'undefined' ? window.location.origin : 'http://localhost:3000');

const api = axios.create({
  baseURL: API_URL,
  headers: {
    'Content-Type': 'application/json',
  },
});

// Add auth token to requests
api.interceptors.request.use((config) => {
  const token = localStorage.getItem('token');
  if (token) {
    config.headers.Authorization = `Bearer ${token}`;
  }
  return config;
});

export interface User {
  id: string;
  email: string;
  created_at: string;
}

export interface Project {
  id: string;
  name: string;
  user_id: string;
  created_at: string;
}

export interface ErrorGroup {
  id: string;
  project_id: string;
  fingerprint: string;
  first_seen: string;
  last_seen: string;
  count: number;
  status: string;
  level: string;
  message: string;
  exception_type?: string;
  exception_value?: string;
}

export interface Error {
  id: string;
  project_id: string;
  fingerprint: string;
  level: string;
  message: string;
  exception_type?: string;
  exception_value?: string;
  stacktrace: any;
  context: any;
  tags: any;
  user_data: any;
  timestamp: string;
  created_at: string;
}

export interface ErrorGroupDetail {
  group: ErrorGroup;
  recent_exceptions: Error[];
}

export interface ProjectStats {
  total_exceptions: number;
  unresolved_exceptions: number;
  resolved_exceptions: number;
  exception_rate_24h: Array<{ time: string; count: number }>;
}

export interface ProjectStatsWithExceptions {
  stats: ProjectStats;
  exception_groups: ErrorGroup[];
}

// Auth API
export const authApi = {
  signup: async (email: string, password: string) => {
    const response = await api.post('/api/auth/signup', { email, password });
    if (response.data.token) {
      localStorage.setItem('token', response.data.token);
    }
    return response.data;
  },
  
  login: async (email: string, password: string) => {
    const response = await api.post('/api/auth/login', { email, password });
    if (response.data.token) {
      localStorage.setItem('token', response.data.token);
    }
    return response.data;
  },
  
  me: async (): Promise<User> => {
    const response = await api.get('/api/auth/me');
    return response.data;
  },
  
  logout: () => {
    localStorage.removeItem('token');
  },
};

// Projects API
export const projectsApi = {
  list: async (): Promise<Project[]> => {
    const response = await api.get('/api/projects');
    return response.data;
  },
  
  get: async (id: string): Promise<Project> => {
    const response = await api.get(`/api/projects/${id}`);
    return response.data;
  },
  
  create: async (name: string): Promise<Project> => {
    const response = await api.post('/api/projects', { name });
    return response.data;
  },
  
  delete: async (id: string) => {
    await api.delete(`/api/projects/${id}`);
  },
  
  getKeys: async (projectId: string) => {
    const response = await api.get(`/api/projects/${projectId}/keys`);
    return response.data;
  },
  
  createKey: async (projectId: string) => {
    const response = await api.post(`/api/projects/${projectId}/keys`);
    return response.data;
  },
  
  getExceptionGroups: async (projectId: string): Promise<ErrorGroup[]> => {
    const response = await api.get(`/api/projects/${projectId}/exceptions`);
    return response.data;
  },
  
  getExceptionGroup: async (projectId: string, groupId: string): Promise<ErrorGroupDetail> => {
    const response = await api.get(`/api/projects/${projectId}/exceptions/${groupId}`);
    return response.data;
  },
  
  getStats: async (projectId: string): Promise<ProjectStats> => {
    const response = await api.get(`/api/projects/${projectId}/stats`);
    return response.data;
  },
};

