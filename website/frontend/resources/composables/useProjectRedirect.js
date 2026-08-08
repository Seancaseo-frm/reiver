import axios from 'axios';
import { registerProjects } from './projectResolver';

const STORAGE_KEY = 'dh_current_project_id';

/**
 * Determines the best project to redirect to after login/signup.
 * Prefers the last-used project (from localStorage), falls back to first available.
 * Returns the path to navigate to, or null if no projects exist.
 */
export async function getProjectRedirectPath() {
  try {
    const projectsRes = await axios.get('/api/projects');
    const projects = projectsRes.data || [];

    if (projects.length === 0) return '/projects/create';

    registerProjects(projects);

    const lastUsedId = localStorage.getItem(STORAGE_KEY);
    const lastUsed = lastUsedId
      ? projects.find((p) => p.id === lastUsedId)
      : null;

    const target = lastUsed || projects[0];
    localStorage.setItem(STORAGE_KEY, target.id);
    return `/p/${target.slug || target.id}/dashboards`;
  } catch {
    return '/';
  }
}
