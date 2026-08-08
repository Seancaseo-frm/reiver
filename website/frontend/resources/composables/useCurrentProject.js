import { ref } from 'vue';
import axios from 'axios';
import { registerProject } from './projectResolver';

const STORAGE_KEY = 'dh_current_project_id';

const currentProject = ref(null);
const isLoading = ref(false);

export function useCurrentProject() {
  const setCurrentProject = (project) => {
    currentProject.value = project;
    if (project?.id) {
      localStorage.setItem(STORAGE_KEY, project.id);
      if (project.slug) {
        registerProject(project.slug, project.id);
      }
    }
  };

  const clearCurrentProject = () => {
    currentProject.value = null;
    localStorage.removeItem(STORAGE_KEY);
  };

  const storedProjectId = () => {
    return localStorage.getItem(STORAGE_KEY);
  };

  const fetchAndSet = async (idOrSlug) => {
    if (!idOrSlug) return;
    const cur = currentProject.value;
    if (cur && (cur.id === idOrSlug || cur.slug === idOrSlug)) return;
    isLoading.value = true;
    try {
      const response = await axios.get(`/api/projects/${idOrSlug}`);
      setCurrentProject(response.data);
    } catch {
      // Project not found or no access -- leave current value intact
    } finally {
      isLoading.value = false;
    }
  };

  const restore = async () => {
    if (currentProject.value) return;
    const id = storedProjectId();
    if (id) {
      await fetchAndSet(id);
      if (currentProject.value) return;
    }
    // Fallback: fetch the user's project list and use the first one
    try {
      const response = await axios.get('/api/projects');
      const projects = response.data;
      if (projects?.length > 0) {
        setCurrentProject(projects[0]);
      }
    } catch {
      // No projects or not authenticated
    }
  };

  return {
    currentProject,
    isLoading,
    setCurrentProject,
    clearCurrentProject,
    storedProjectId,
    fetchAndSet,
    restore,
  };
}
