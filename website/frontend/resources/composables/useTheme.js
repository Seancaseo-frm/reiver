import { ref, onMounted, watch } from 'vue'

const isDark = ref(false)

export function useTheme() {
  const initTheme = () => {
    localStorage.removeItem('theme')
    isDark.value = false
    applyTheme()
  }

  // Apply theme to HTML element
  const applyTheme = () => {
    if (isDark.value) {
      document.documentElement.classList.add('dark')
    } else {
      document.documentElement.classList.remove('dark')
    }
  }

  // Toggle theme
  const toggleTheme = () => {
    isDark.value = !isDark.value
    localStorage.setItem('theme', isDark.value ? 'dark' : 'light')
    applyTheme()
  }

  // Set theme explicitly
  const setTheme = (theme) => {
    isDark.value = theme === 'dark'
    localStorage.setItem('theme', theme)
    applyTheme()
  }

  // Watch for system theme changes
  const watchSystemTheme = () => {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
    const handleChange = (e) => {
      if (!localStorage.getItem('theme')) {
        // Only auto-switch if user hasn't set a preference
        isDark.value = e.matches
        applyTheme()
      }
    }
    mediaQuery.addEventListener('change', handleChange)
    return () => mediaQuery.removeEventListener('change', handleChange)
  }

  onMounted(() => {
    initTheme()
    watchSystemTheme()
  })

  // Watch for changes
  watch(isDark, applyTheme)

  return {
    isDark,
    toggleTheme,
    setTheme,
  }
}


