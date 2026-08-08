import { ref } from 'vue';

const pageSnapshot = ref(null);

export function usePageContext() {
  const setPageSnapshot = (data) => {
    pageSnapshot.value = data;
  };

  const clearPageSnapshot = () => {
    pageSnapshot.value = null;
  };

  return {
    pageSnapshot,
    setPageSnapshot,
    clearPageSnapshot,
  };
}
