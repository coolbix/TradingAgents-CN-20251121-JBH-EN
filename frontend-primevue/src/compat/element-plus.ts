import type { VNode, Component } from 'vue'
import { h, defineComponent } from 'vue'
import Button from 'primevue/button'
import InputText from 'primevue/inputtext'
import InputNumber from 'primevue/inputnumber'
import { getPrimeConfirmService, getPrimeToastService } from './primevue-services'

type MessageOptions = {
  message: string
  type?: 'success' | 'info' | 'warning' | 'error'
  duration?: number
}

const resolveSeverity = (type?: MessageOptions['type']) => {
  switch (type) {
    case 'success':
      return 'success'
    case 'warning':
      return 'warn'
    case 'error':
      return 'error'
    default:
      return 'info'
  }
}

const showToast = (options: MessageOptions) => {
  const toast = getPrimeToastService()
  if (!toast) {
    // Fallback when toast is unavailable.
    console.log(options.message)
    return
  }
  toast.add({
    severity: resolveSeverity(options.type),
    detail: options.message,
    life: options.duration ?? 3000
  })
}

export const ElMessage = Object.assign(
  (options: MessageOptions | string) => {
    if (typeof options === 'string') {
      showToast({ message: options })
      return
    }
    showToast(options)
  },
  {
    success: (message: string, duration?: number) => showToast({ message, type: 'success', duration }),
    warning: (message: string, duration?: number) => showToast({ message, type: 'warning', duration }),
    info: (message: string, duration?: number) => showToast({ message, type: 'info', duration }),
    error: (message: string, duration?: number) => showToast({ message, type: 'error', duration })
  }
)

type ConfirmOptions = {
  type?: 'success' | 'info' | 'warning' | 'error'
  confirmButtonText?: string
  cancelButtonText?: string
}

type MessageBoxOptions = {
  title?: string
  message?: string | VNode
  type?: 'success' | 'info' | 'warning' | 'error'
  showCancelButton?: boolean
}

const normalizeMessage = (message?: string | VNode) => {
  if (!message) return ''
  if (typeof message === 'string') return message
  return '[内容已简化显示]'
}

const showConfirm = (message: string, title?: string, options?: ConfirmOptions) => {
  const confirm = getPrimeConfirmService()
  if (!confirm) {
    const ok = window.confirm(message)
    return ok ? Promise.resolve('confirm') : Promise.reject('cancel')
  }

  return new Promise((resolve, reject) => {
    confirm.require({
      header: title || '确认',
      message,
      rejectLabel: options?.cancelButtonText || '取消',
      acceptLabel: options?.confirmButtonText || '确定',
      accept: () => resolve('confirm'),
      reject: () => reject('cancel')
    })
  })
}

export const ElMessageBox = Object.assign(
  (options: MessageBoxOptions) => {
    const message = normalizeMessage(options.message)
    if (options.showCancelButton) {
      return showConfirm(message, options.title, { type: options.type })
    }
    window.alert(message)
    return Promise.resolve('confirm')
  },
  {
    confirm: (message: string, title?: string, options?: ConfirmOptions) => showConfirm(message, title, options),
    alert: (message: string, title?: string) => {
      window.alert(message)
      return Promise.resolve('confirm')
    },
    prompt: (message: string, title?: string) => {
      const value = window.prompt(message, '')
      if (value === null) {
        return Promise.reject('cancel')
      }
      return Promise.resolve({ value })
    }
  }
)

export type FormInstance = Record<string, unknown>
export type FormRules = Record<string, unknown>

// Simple form wrappers for Element Plus compatibility
export const ElForm = defineComponent({
  name: 'ElForm',
  props: {
    model: Object,
    rules: Object,
    labelWidth: String,
    labelPosition: String
  },
  setup(props, { slots }) {
    return () => h('form', { class: 'el-form' }, slots.default?.())
  }
})

export const ElFormItem = defineComponent({
  name: 'ElFormItem',
  props: {
    label: String,
    prop: String,
    required: Boolean
  },
  setup(props, { slots }) {
    return () => h('div', { class: 'el-form-item' }, [
      props.label && h('label', { class: 'el-form-item__label' }, props.label),
      h('div', { class: 'el-form-item__content' }, slots.default?.())
    ])
  }
})

export const ElInput = InputText
export const ElInputNumber = InputNumber

export default {
  ElMessage,
  ElMessageBox,
  ElInput,
  ElInputNumber,
  ElForm,
  ElFormItem
}
