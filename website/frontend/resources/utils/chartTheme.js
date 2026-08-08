/**
 * Chart Theme Configuration
 * Provides consistent colors and styling for Chart.js charts
 */

export const chartTheme = {
  colors: {
    primary: '#4f46e5',
    primaryHover: '#4338ca',
    primaryLight: 'rgba(79, 70, 229, 0.1)',

    success: '#22c55e',
    error: '#ef4444',
    warning: '#f59e0b',
    info: '#3b82f6',

    bgPrimary: '#ffffff',
    bgSecondary: '#f9fafb',
    bgTertiary: '#f3f4f6',

    border: '#e5e7eb',
    borderLight: '#f3f4f6',

    textPrimary: '#111827',
    textSecondary: '#6b7280',
    textTertiary: '#9ca3af',

    lineColors: [
      '#4f46e5',
      '#22c55e',
      '#f59e0b',
      '#ef4444',
      '#3b82f6',
      '#8b5cf6',
      '#ec4899',
      '#14b8a6',
      '#f97316',
      '#6366f1',
    ],

    gradients: {
      primary: {
        from: 'rgba(79, 70, 229, 0.2)',
        to: 'rgba(79, 70, 229, 0.05)',
      },
      success: {
        from: 'rgba(34, 197, 94, 0.2)',
        to: 'rgba(34, 197, 94, 0.05)',
      },
      error: {
        from: 'rgba(239, 68, 68, 0.2)',
        to: 'rgba(239, 68, 68, 0.05)',
      },
      warning: {
        from: 'rgba(245, 158, 11, 0.2)',
        to: 'rgba(245, 158, 11, 0.05)',
      },
    },
  },

  plugins: {
    legend: {
      labels: {
        color: '#6b7280',
        font: {
          family: "'Inter', system-ui, sans-serif",
          size: 12,
        },
        padding: 12,
        usePointStyle: true,
        pointStyle: 'circle',
      },
      position: 'top',
      align: 'end',
    },
    tooltip: {
      backgroundColor: '#ffffff',
      titleColor: '#111827',
      bodyColor: '#6b7280',
      borderColor: '#e5e7eb',
      borderWidth: 1,
      padding: 12,
      titleFont: {
        family: "'Inter', system-ui, sans-serif",
        size: 13,
        weight: 600,
      },
      bodyFont: {
        family: "'Inter', system-ui, sans-serif",
        size: 12,
      },
      cornerRadius: 6,
      displayColors: true,
    },
  },

  commonOptions: {
    responsive: true,
    maintainAspectRatio: false,
    plugins: {
      legend: {
        display: true,
        labels: {
          color: '#6b7280',
          font: {
            family: "'Inter', system-ui, sans-serif",
            size: 12,
          },
          padding: 12,
          usePointStyle: true,
          pointStyle: 'circle',
        },
        position: 'top',
        align: 'end',
      },
      tooltip: {
        backgroundColor: '#ffffff',
        titleColor: '#111827',
        bodyColor: '#6b7280',
        borderColor: '#e5e7eb',
        borderWidth: 1,
        padding: 12,
        titleFont: {
          family: "'Inter', system-ui, sans-serif",
          size: 13,
          weight: 600,
        },
        bodyFont: {
          family: "'Inter', system-ui, sans-serif",
          size: 12,
        },
        cornerRadius: 6,
        displayColors: true,
      },
    },
    scales: {
      x: {
        grid: {
          color: '#f3f4f6',
          lineWidth: 1,
        },
        ticks: {
          color: '#9ca3af',
          font: {
            family: "'Inter', system-ui, sans-serif",
            size: 11,
          },
        },
        border: {
          color: '#e5e7eb',
        },
      },
      y: {
        grid: {
          color: '#f3f4f6',
          lineWidth: 1,
        },
        ticks: {
          color: '#9ca3af',
          font: {
            family: "'Inter', system-ui, sans-serif",
            size: 11,
          },
        },
        border: {
          color: '#e5e7eb',
        },
      },
    },
  },

  getColor(index) {
    return this.colors.lineColors[index % this.colors.lineColors.length]
  },

  hexToRgba(hex, alpha = 1) {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex)
    if (!result) return hex
    const r = parseInt(result[1], 16)
    const g = parseInt(result[2], 16)
    const b = parseInt(result[3], 16)
    return `rgba(${r}, ${g}, ${b}, ${alpha})`
  },

  createGradient(ctx, gradientType = 'primary') {
    const gradient = ctx.createLinearGradient(0, 0, 0, 400)
    const grad = this.colors.gradients[gradientType]
    if (!grad) {
      throw new Error(`Gradient type ${gradientType} not found`)
    }
    gradient.addColorStop(0, grad.from)
    gradient.addColorStop(1, grad.to)
    return gradient
  },
}

export function getDefaultChartConfig() {
  return {
    ...chartTheme.commonOptions,
    plugins: {
      ...chartTheme.plugins,
    },
  }
}

export function applyChartTheme(options = {}) {
  return {
    ...chartTheme.commonOptions,
    ...options,
    plugins: {
      ...chartTheme.plugins,
      ...(options.plugins || {}),
    },
  }
}
