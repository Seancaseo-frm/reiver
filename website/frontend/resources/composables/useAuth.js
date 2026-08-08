import { ref } from 'vue';
import axios from 'axios';

// Global user state
const user = ref(null);
const isLoading = ref(false);

export function useAuth() {
  const fetchUser = async () => {
    // Return cached user if available
    if (user.value) {
      return user.value;
    }

    // Fetch user if not cached
    isLoading.value = true;
    try {
      const response = await axios.get('/api/auth/me');
      user.value = response.data;
      return user.value;
    } catch (error) {
      user.value = null;
      throw error;
    } finally {
      isLoading.value = false;
    }
  };

  const setUser = (userData) => {
    user.value = userData;
  };

  const clearUser = () => {
    user.value = null;
  };

  return {
    user,
    isLoading,
    fetchUser,
    setUser,
    clearUser,
  };
}


