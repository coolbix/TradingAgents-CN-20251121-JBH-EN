import { defineComponent, h, withDirectives, resolveDirective, provide, inject, ref, computed, type App, type VNode } from 'vue'
import type { Directive } from 'vue'
import dayjs from 'dayjs'
import { useRoute, useRouter } from 'vue-router'
import Button from 'primevue/button'
import Card from 'primevue/card'
import Tag from 'primevue/tag'
import ProgressBar from 'primevue/progressbar'
import Message from 'primevue/message'
import Divider from 'primevue/divider'
import InputText from 'primevue/inputtext'
import InputNumber from 'primevue/inputnumber'
import InputSwitch from 'primevue/inputswitch'
import Checkbox from 'primevue/checkbox'
import RadioButton from 'primevue/radiobutton'
import Slider from 'primevue/slider'
import Select from 'primevue/select'
import MultiSelect from 'primevue/multiselect'
import Dialog from 'primevue/dialog'
import DataTable from 'primevue/datatable'
import Column from 'primevue/column'
import TabView from 'primevue/tabview'
import TabPanel from 'primevue/tabpanel'
import Paginator from 'primevue/paginator'
import Badge from 'primevue/badge'
import Drawer from 'primevue/drawer'
import Avatar from 'primevue/avatar'
import Skeleton from 'primevue/skeleton'
import Timeline from 'primevue/timeline'
import Steps from 'primevue/steps'
import Accordion from 'primevue/accordion'
import AccordionTab from 'primevue/accordiontab'
import FileUpload from 'primevue/fileupload'
import Tooltip from 'primevue/tooltip'
import SelectButton from 'primevue/selectbutton'
import DatePicker from 'primevue/datepicker'
import ScrollTop from 'primevue/scrolltop'

const ElIcon = defineComponent({
  name: 'ElIcon',
  setup(_, { slots }) {
    return () => h('span', { class: 'el-icon' }, slots.default?.())
  }
})

const ElButton = defineComponent({
  name: 'ElButton',
  props: {
    type: { type: String, default: '' },
    icon: { type: [Object, Function], default: null },
    loading: { type: Boolean, default: false },
    disabled: { type: Boolean, default: false }
  },
  setup(props, { slots }) {
    const severityMap: Record<string, string> = {
      primary: 'primary',
      success: 'success',
      warning: 'warn',
      danger: 'danger',
      info: 'info'
    }
    return () =>
      h(
        Button,
        {
          severity: severityMap[props.type] || undefined,
          loading: props.loading,
          disabled: props.disabled
        },
        {
          default: slots.default,
          icon: props.icon ? () => h(props.icon as any) : undefined
        }
      )
  }
})

const ElCard = defineComponent({
  name: 'ElCard',
  props: {
    header: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () =>
      h(
        Card,
        {},
        {
          header: slots.header,
          title: props.header ? () => props.header : undefined,
          content: slots.default
        }
      )
  }
})

const ElTag = defineComponent({
  name: 'ElTag',
  props: {
    type: { type: String, default: '' }
  },
  setup(props, { slots }) {
    const severityMap: Record<string, string> = {
      success: 'success',
      warning: 'warn',
      danger: 'danger',
      info: 'info',
      primary: 'primary'
    }
    return () => h(Tag, { severity: severityMap[props.type] || undefined }, slots)
  }
})

const ElAlert = defineComponent({
  name: 'ElAlert',
  props: {
    title: { type: String, default: '' },
    type: { type: String, default: 'info' }
  },
  setup(props, { slots }) {
    const severityMap: Record<string, string> = {
      success: 'success',
      warning: 'warn',
      error: 'error',
      info: 'info'
    }
    return () =>
      h(
        Message,
        { severity: severityMap[props.type] || 'info' },
        () => props.title || slots.default?.()
      )
  }
})

const ElProgress = defineComponent({
  name: 'ElProgress',
  props: {
    percentage: { type: Number, default: 0 }
  },
  setup(props) {
    return () => h(ProgressBar, { value: props.percentage })
  }
})

const ElText = defineComponent({
  name: 'ElText',
  setup(_, { slots }) {
    return () => h('span', { class: 'el-text' }, slots.default?.())
  }
})

const ElDialog = defineComponent({
  name: 'ElDialog',
  props: {
    modelValue: { type: Boolean, default: false },
    title: { type: String, default: '' },
    width: { type: [String, Number], default: '' },
    modal: { type: Boolean, default: true },
    closeOnClickModal: { type: Boolean, default: true }
  },
  emits: ['update:modelValue'],
  setup(props, { emit, slots }) {
    return () =>
      h(
        Dialog,
        {
          visible: props.modelValue,
          header: props.title,
          modal: props.modal,
          dismissableMask: props.closeOnClickModal,
          style: props.width ? { width: String(props.width) } : undefined,
          'onUpdate:visible': (value: boolean) => emit('update:modelValue', value)
        },
        slots
      )
  }
})

const ElSelect = defineComponent({
  name: 'ElSelect',
  props: {
    modelValue: { type: [String, Number, Array, Object], default: null },
    placeholder: { type: String, default: '' },
    disabled: { type: Boolean, default: false },
    clearable: { type: Boolean, default: false },
    filterable: { type: Boolean, default: false },
    multiple: { type: Boolean, default: false }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit, slots }) {
    const buildOptions = () => {
      const nodes = slots.default?.() || []
      return nodes
        .filter((node) => node.type && typeof node.type === 'object' && (node.type as any).name === 'ElOption')
        .map((node) => {
          const optionProps = (node.props || {}) as Record<string, any>
          return {
            label: optionProps.label,
            value: optionProps.value,
            disabled: optionProps.disabled
          }
        })
    }

    return () => {
      const options = buildOptions()
      const component = props.multiple ? MultiSelect : Select
      return h(component, {
        modelValue: props.modelValue,
        options,
        optionLabel: 'label',
        optionValue: 'value',
        optionDisabled: 'disabled',
        placeholder: props.placeholder,
        disabled: props.disabled,
        showClear: props.clearable,
        filter: props.filterable,
        'onUpdate:modelValue': (value: any) => emit('update:modelValue', value),
        onChange: (event: any) => emit('change', event?.value ?? event)
      })
    }
  }
})

const ElOption = defineComponent({
  name: 'ElOption',
  props: {
    label: { type: String, default: '' },
    value: { type: [String, Number, Boolean, Object], default: null },
    disabled: { type: Boolean, default: false }
  },
  setup() {
    return () => null
  }
})

const CheckboxGroupKey = Symbol('ElCheckboxGroup')
const RadioGroupKey = Symbol('ElRadioGroup')

const ElCheckboxGroup = defineComponent({
  name: 'ElCheckboxGroup',
  props: {
    modelValue: { type: Array, default: () => [] }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit, slots }) {
    const update = (value: any[]) => {
      emit('update:modelValue', value)
      emit('change', value)
    }
    provide(CheckboxGroupKey, { value: () => props.modelValue, update })
    return () => h('div', { class: 'el-checkbox-group' }, slots.default?.())
  }
})

const ElCheckbox = defineComponent({
  name: 'ElCheckbox',
  props: {
    label: { type: [String, Number, Boolean], default: '' },
    modelValue: { type: Boolean, default: undefined }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit, slots }) {
    const group = inject<{ value: () => any[]; update: (value: any[]) => void } | null>(CheckboxGroupKey, null)
    if (group) {
      return () =>
        h(Checkbox, {
          modelValue: group.value(),
          value: props.label,
          'onUpdate:modelValue': (value: any[]) => group.update(value)
        }, slots)
    }
    return () =>
      h(Checkbox, {
        modelValue: props.modelValue,
        binary: true,
        'onUpdate:modelValue': (value: boolean) => {
          emit('update:modelValue', value)
          emit('change', value)
        }
      }, slots)
  }
})

const ElRadioGroup = defineComponent({
  name: 'ElRadioGroup',
  props: {
    modelValue: { type: [String, Number, Boolean], default: '' }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit, slots }) {
    const update = (value: any) => {
      emit('update:modelValue', value)
      emit('change', value)
    }
    provide(RadioGroupKey, { value: () => props.modelValue, update })
    return () => h('div', { class: 'el-radio-group' }, slots.default?.())
  }
})

const ElRadio = defineComponent({
  name: 'ElRadio',
  props: {
    label: { type: [String, Number, Boolean], default: '' }
  },
  setup(props, { slots }) {
    const group = inject<{ value: () => any; update: (value: any) => void } | null>(RadioGroupKey, null)
    return () =>
      h(RadioButton, {
        modelValue: group?.value(),
        value: props.label,
        'onUpdate:modelValue': (value: any) => group?.update(value)
      }, slots)
  }
})

const ElTable = defineComponent({
  name: 'ElTable',
  props: {
    data: { type: Array, default: () => [] },
    stripe: { type: Boolean, default: false },
    border: { type: Boolean, default: false },
    height: { type: [String, Number], default: '' },
    maxHeight: { type: [String, Number], default: '' }
  },
  setup(props, { slots }) {
    const scrollHeight = props.height || props.maxHeight
    return () =>
      h(
        DataTable,
        {
          value: props.data,
          stripedRows: props.stripe,
          showGridlines: props.border,
          scrollable: !!scrollHeight,
          scrollHeight: scrollHeight ? String(scrollHeight) : undefined
        },
        slots
      )
  }
})

const ElTableColumn = defineComponent({
  name: 'ElTableColumn',
  props: {
    prop: { type: String, default: '' },
    label: { type: String, default: '' },
    width: { type: [String, Number], default: '' }
  },
  setup(props, { slots }) {
    return () =>
      h(
        Column,
        {
          field: props.prop,
          header: props.label,
          style: props.width ? { width: String(props.width) } : undefined
        },
        slots
      )
  }
})

const ElTabs = defineComponent({
  name: 'ElTabs',
  props: {
    modelValue: { type: String, default: '' }
  },
  emits: ['update:modelValue', 'tab-click'],
  setup(props, { emit, slots }) {
    return () =>
      h(
        TabView,
        {
          activeIndex: slots.default?.().findIndex((pane) => (pane.props as any)?.name === props.modelValue),
          'onUpdate:activeIndex': (index: number) => {
            const panes = slots.default?.() || []
            const name = (panes[index]?.props as any)?.name
            if (name) emit('update:modelValue', name)
          },
          onTabClick: (event: any) => emit('tab-click', event)
        },
        slots
      )
  }
})

const ElTabPane = defineComponent({
  name: 'ElTabPane',
  props: {
    name: { type: String, default: '' },
    label: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () => h(TabPanel, { header: props.label }, slots)
  }
})

const ElPagination = defineComponent({
  name: 'ElPagination',
  props: {
    currentPage: { type: Number, default: 1 },
    pageSize: { type: Number, default: 10 },
    total: { type: Number, default: 0 }
  },
  emits: ['update:currentPage', 'update:pageSize', 'current-change', 'size-change'],
  setup(props, { emit }) {
    return () =>
      h(Paginator, {
        first: (props.currentPage - 1) * props.pageSize,
        rows: props.pageSize,
        totalRecords: props.total,
        'onUpdate:first': (value: number) => {
          const page = Math.floor(value / props.pageSize) + 1
          emit('update:currentPage', page)
          emit('current-change', page)
        },
        'onUpdate:rows': (value: number) => {
          emit('update:pageSize', value)
          emit('size-change', value)
        }
      })
  }
})

const ElRow = defineComponent({
  name: 'ElRow',
  setup(_, { slots }) {
    return () => h('div', { class: 'grid' }, slots.default?.())
  }
})

const ElCol = defineComponent({
  name: 'ElCol',
  props: {
    span: { type: Number, default: 24 }
  },
  setup(props, { slots }) {
    const span = Math.min(Math.max(props.span, 1), 24)
    const col = Math.round((span / 24) * 12)
    return () => h('div', { class: `col-${col}` }, slots.default?.())
  }
})

const ElTooltip = defineComponent({
  name: 'ElTooltip',
  props: {
    content: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () => {
      const tooltip = resolveDirective('tooltip')
      const contentNode = slots.content?.()[0] as VNode | undefined
      const content =
        props.content || (contentNode && typeof contentNode.children === 'string' ? contentNode.children : '')
      if (!tooltip) {
        return h('span', slots.default?.())
      }
      return withDirectives(h('span', { class: 'el-tooltip-trigger' }, slots.default?.()), [
        [tooltip, content]
      ])
    }
  }
})

const ElEmpty = defineComponent({
  name: 'ElEmpty',
  props: {
    description: { type: String, default: '暂无数据' }
  },
  setup(props) {
    return () => h('div', { class: 'el-empty' }, props.description)
  }
})

const ElResult = defineComponent({
  name: 'ElResult',
  props: {
    title: { type: String, default: '' },
    subTitle: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () =>
      h('div', { class: 'el-result' }, [
        h('h3', { class: 'el-result__title' }, props.title),
        h('p', { class: 'el-result__subtitle' }, props.subTitle),
        slots.extra ? h('div', { class: 'el-result__extra' }, slots.extra()) : null
      ])
  }
})

const ElSteps = defineComponent({
  name: 'ElSteps',
  props: {
    active: { type: Number, default: 0 }
  },
  setup(props, { slots }) {
    const items = (slots.default?.() || []).map((node) => ({
      label: (node.props as any)?.title || ''
    }))
    return () => h(Steps, { modelValue: props.active, items })
  }
})

const ElStep = defineComponent({
  name: 'ElStep',
  props: {
    title: { type: String, default: '' }
  },
  setup() {
    return () => null
  }
})

const ElTimeline = defineComponent({
  name: 'ElTimeline',
  setup(_, { slots }) {
    const events = (slots.default?.() || []).map((node) => ({
      status: (node.props as any)?.timestamp,
      content: node
    }))
    return () => h(Timeline, { value: events }, { content: ({ item }: any) => item.content })
  }
})

const ElTimelineItem = defineComponent({
  name: 'ElTimelineItem',
  props: {
    timestamp: { type: String, default: '' }
  },
  setup(_, { slots }) {
    return () => h('div', slots.default?.())
  }
})

const ElCollapse = defineComponent({
  name: 'ElCollapse',
  setup(_, { slots }) {
    return () => h(Accordion, {}, slots)
  }
})

const ElCollapseItem = defineComponent({
  name: 'ElCollapseItem',
  props: {
    title: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () => h(AccordionTab, { header: props.title }, slots)
  }
})

type MenuContext = {
  collapse: boolean
  uniqueOpened: boolean
  router: boolean
  defaultActive: string
  activePath: () => string
  toggleOpen: (key: string) => void
  isOpened: (key: string) => boolean
  registerSubMenu: (key: string, parentKey: string | null) => void
}

const menuContextKey = Symbol('el-menu-context')
const subMenuParentKey = Symbol('el-sub-menu-parent')

const ElMenu = defineComponent({
  name: 'ElMenu',
  props: {
    defaultActive: { type: String, default: '' },
    collapse: { type: Boolean, default: false },
    uniqueOpened: { type: Boolean, default: false },
    router: { type: Boolean, default: false }
  },
  setup(props, { slots }) {
    const route = useRoute()
    const activePath = computed(() => props.defaultActive || route.fullPath)
    const openedKeys = ref<Set<string>>(new Set())
    const parentMap = ref<Map<string, string | null>>(new Map())
    const registerSubMenu = (key: string, parentKey: string | null) => {
      if (!key) return
      parentMap.value.set(key, parentKey)
    }
    const toggleOpen = (key: string) => {
      if (!key) return
      const parentKey = parentMap.value.get(key) ?? null
      const next = new Set(openedKeys.value)
      if (next.has(key)) {
        next.delete(key)
      } else {
        if (props.uniqueOpened) {
          for (const openedKey of next) {
            const openedParent = parentMap.value.get(openedKey) ?? null
            if (openedParent === parentKey) {
              next.delete(openedKey)
            }
          }
        }
        next.add(key)
      }
      openedKeys.value = next
    }
    const isOpened = (key: string) => openedKeys.value.has(key)

    provide<MenuContext>(menuContextKey, {
      collapse: props.collapse,
      uniqueOpened: props.uniqueOpened,
      router: props.router,
      defaultActive: props.defaultActive,
      activePath: () => activePath.value,
      toggleOpen,
      isOpened,
      registerSubMenu
    })

    return () =>
      h(
        'ul',
        {
          class: ['el-menu', props.collapse ? 'el-menu--collapse' : '', 'el-menu--vertical']
        },
        slots.default?.()
      )
  }
})

const ElMenuItem = defineComponent({
  name: 'ElMenuItem',
  props: {
    index: { type: String, default: '' }
  },
  setup(props, { slots }) {
    const menu = inject<MenuContext | null>(menuContextKey, null)
    const router = useRouter()
    const route = useRoute()
    const activeKey = computed(() => menu?.activePath() || route.fullPath)
    const isActive = computed(() => !!props.index && activeKey.value === props.index)
    const handleClick = () => {
      if (!props.index) return
      if (menu?.router) {
        router.push(props.index)
      }
    }

    return () =>
      h(
        'li',
        {
          class: ['el-menu-item', isActive.value ? 'is-active' : ''],
          'data-index': props.index
        },
        [
          h(
            'button',
            { class: 'el-menu-item__content', type: 'button', onClick: handleClick },
            [
              slots.default?.(),
              slots.title ? h('span', { class: 'el-menu-item__title' }, slots.title()) : null
            ]
          )
        ]
      )
  }
})

const findActiveIndex = (nodes: VNode[] | undefined, activePath: string) => {
  if (!nodes) return false
  for (const node of nodes) {
    if (!node) continue
    const props = (node.props || {}) as Record<string, any>
    if (props.index && props.index === activePath) return true
    const children = (node.children || []) as VNode[] | undefined
    if (Array.isArray(children) && findActiveIndex(children, activePath)) return true
    const defaultSlot = (node.children as any)?.default?.()
    if (Array.isArray(defaultSlot) && findActiveIndex(defaultSlot, activePath)) return true
  }
  return false
}

const ElSubMenu = defineComponent({
  name: 'ElSubMenu',
  props: {
    index: { type: String, default: '' }
  },
  setup(props, { slots }) {
    const menu = inject<MenuContext | null>(menuContextKey, null)
    const parentKey = inject<string | null>(subMenuParentKey, null)
    const localOpen = ref(false)
    const hasActiveChild = computed(() => {
      const activePath = menu?.activePath() || ''
      return !!activePath && findActiveIndex(slots.default?.() as VNode[] | undefined, activePath)
    })
    const isOpened = computed(() => {
      if (menu) {
        if (menu.isOpened(props.index)) return true
        if (hasActiveChild.value) return true
        if (props.index && menu.activePath().startsWith(props.index)) return true
        return false
      }
      return localOpen.value || hasActiveChild.value
    })
    const handleToggle = () => {
      if (!props.index) return
      if (menu) {
        menu.toggleOpen(props.index)
      } else {
        localOpen.value = !localOpen.value
      }
    }
    if (menu) {
      menu.registerSubMenu(props.index, parentKey)
    }
    provide(subMenuParentKey, props.index || parentKey)

    return () =>
      h('li', { class: ['el-sub-menu', isOpened.value ? 'is-opened' : ''], 'data-index': props.index }, [
        h('div', { class: 'el-sub-menu__title', onClick: handleToggle }, [
          slots.title?.(),
          h('span', { class: 'el-sub-menu__icon-arrow' }, '▾')
        ]),
        h(
          'ul',
          { class: 'el-menu el-menu--inline', style: { display: isOpened.value ? 'block' : 'none' } },
          slots.default?.()
        )
      ])
  }
})

const ElForm = defineComponent({
  name: 'ElForm',
  setup(_, { slots }) {
    return () => h('form', { class: 'el-form' }, slots.default?.())
  }
})

const ElFormItem = defineComponent({
  name: 'ElFormItem',
  props: {
    label: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () =>
      h('div', { class: 'el-form-item' }, [
        props.label ? h('label', { class: 'el-form-item__label' }, props.label) : null,
        h('div', { class: 'el-form-item__content' }, slots.default?.())
      ])
  }
})

const ElDropdown = defineComponent({
  name: 'ElDropdown',
  setup(_, { slots }) {
    return () => h('div', { class: 'el-dropdown' }, [
      h('span', { class: 'el-dropdown__trigger' }, slots.default?.()),
      slots.dropdown ? h('div', { class: 'el-dropdown__menu' }, slots.dropdown()) : null
    ])
  }
})

const ElDropdownMenu = defineComponent({
  name: 'ElDropdownMenu',
  setup(_, { slots }) {
    return () => h('ul', { class: 'el-dropdown-menu' }, slots.default?.())
  }
})

const ElDropdownItem = defineComponent({
  name: 'ElDropdownItem',
  props: {
    command: { type: String, default: '' },
    divided: { type: Boolean, default: false }
  },
  setup(props, { slots }) {
    return () =>
      h('li', { class: ['el-dropdown-item', props.divided ? 'is-divided' : ''] }, slots.default?.())
  }
})

const ElLink = defineComponent({
  name: 'ElLink',
  props: {
    href: { type: String, default: '' },
    target: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () => h('a', { class: 'el-link', href: props.href, target: props.target }, slots.default?.())
  }
})

const ElDescriptions = defineComponent({
  name: 'ElDescriptions',
  setup(_, { slots }) {
    return () => h('div', { class: 'el-descriptions' }, slots.default?.())
  }
})

const ElDescriptionsItem = defineComponent({
  name: 'ElDescriptionsItem',
  props: {
    label: { type: String, default: '' }
  },
  setup(props, { slots }) {
    return () => h('div', { class: 'el-descriptions-item' }, [
      h('span', { class: 'el-descriptions-item__label' }, props.label),
      h('span', { class: 'el-descriptions-item__content' }, slots.default?.())
    ])
  }
})

const ElBreadcrumb = defineComponent({
  name: 'ElBreadcrumb',
  setup(_, { slots }) {
    return () => h('nav', { class: 'el-breadcrumb' }, slots.default?.())
  }
})

const ElBreadcrumbItem = defineComponent({
  name: 'ElBreadcrumbItem',
  setup(_, { slots }) {
    return () => h('span', { class: 'el-breadcrumb__item' }, slots.default?.())
  }
})

const ElBacktop = defineComponent({
  name: 'ElBacktop',
  setup() {
    return () => h(ScrollTop)
  }
})

const ElPageHeader = defineComponent({
  name: 'ElPageHeader',
  setup(_, { slots }) {
    return () => h('div', { class: 'el-page-header' }, slots.default?.())
  }
})

const ElStatistic = defineComponent({
  name: 'ElStatistic',
  props: {
    title: { type: String, default: '' },
    value: { type: [String, Number], default: '' }
  },
  setup(props, { slots }) {
    return () => h('div', { class: 'el-statistic' }, [
      h('span', { class: 'el-statistic__title' }, props.title),
      h('span', { class: 'el-statistic__value' }, slots.default ? slots.default() : props.value)
    ])
  }
})

const ElSegmented = defineComponent({
  name: 'ElSegmented',
  props: {
    modelValue: { type: [String, Number], default: '' },
    options: { type: Array, default: () => [] }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit }) {
    return () =>
      h(SelectButton, {
        modelValue: props.modelValue,
        options: props.options,
        'onUpdate:modelValue': (value: any) => emit('update:modelValue', value),
        onChange: (event: any) => emit('change', event?.value ?? event)
      })
  }
})

const ElScrollbar = defineComponent({
  name: 'ElScrollbar',
  setup(_, { slots }) {
    return () => h('div', { class: 'el-scrollbar' }, slots.default?.())
  }
})

const ElDatePicker = defineComponent({
  name: 'ElDatePicker',
  props: {
    modelValue: { type: [String, Date, Array], default: null },
    type: { type: String, default: 'date' },
    placeholder: { type: String, default: '' },
    format: { type: String, default: '' },
    valueFormat: { type: String, default: '' }
  },
  emits: ['update:modelValue', 'change'],
  setup(props, { emit }) {
    const toDate = (value: unknown) => {
      if (!value) return null
      if (value instanceof Date) return value
      const parsed = new Date(String(value))
      return Number.isNaN(parsed.getTime()) ? null : parsed
    }
    const toCalendarValue = (value: unknown) => {
      if (props.type === 'daterange' || props.type === 'datetimerange') {
        const range = Array.isArray(value) ? value : [value, null]
        const now = new Date()
        const start = toDate(range[0]) || now
        const end = toDate(range[1]) || now
        return [start, end]
      }
      return toDate(value) || new Date()
    }
    const formatValue = (value: Date | Date[] | null) => {
      if (!props.valueFormat) return value
      if (!value) return value
      if (Array.isArray(value)) {
        return value.map((item) => dayjs(item).format(props.valueFormat))
      }
      return dayjs(value).format(props.valueFormat)
    }
    const selectionMode =
      props.type === 'daterange' || props.type === 'datetimerange' ? 'range' : 'single'
    return () =>
      h(DatePicker, {
        modelValue: toCalendarValue(props.modelValue),
        selectionMode,
        placeholder: props.placeholder,
        dateFormat: props.format || undefined,
        showTime: props.type === 'datetimerange',
        'onUpdate:modelValue': (value: any) => emit('update:modelValue', formatValue(value)),
        onChange: (event: any) => emit('change', formatValue(event?.value ?? event))
      })
  }
})

const ElPopconfirm = defineComponent({
  name: 'ElPopconfirm',
  props: {
    title: { type: String, default: '' }
  },
  emits: ['confirm'],
  setup(props, { emit, slots }) {
    const onClick = () => {
      if (window.confirm(props.title || '确认操作？')) {
        emit('confirm')
      }
    }
    return () => h('span', { class: 'el-popconfirm', onClick }, slots.reference?.() || slots.default?.())
  }
})

const ElButtonGroup = defineComponent({
  name: 'ElButtonGroup',
  setup(_, { slots }) {
    return () => h('div', { class: 'el-button-group' }, slots.default?.())
  }
})

const ElAvatar = defineComponent({
  name: 'ElAvatar',
  props: {
    size: { type: [String, Number], default: undefined },
    src: { type: String, default: '' },
    alt: { type: String, default: '' }
  },
  setup(props, { slots }) {
    const normalizedSize =
      typeof props.size === 'number' ? String(props.size) : props.size || undefined
    return () =>
      h(
        Avatar,
        {
          size: normalizedSize,
          image: props.src || undefined,
          alt: props.alt || undefined
        },
        slots
      )
  }
})

export const setupPrimeVueCompat = (app: App) => {
  const loadingDirective: Directive = {
    mounted(el, binding) {
      const overlay = document.createElement('div')
      overlay.className = 'el-loading-mask'
      overlay.innerHTML = '<span class="el-loading-spinner"></span>'
      overlay.style.display = binding.value ? 'flex' : 'none'
      el.dataset.elLoading = 'true'
      el.appendChild(overlay)
    },
    updated(el, binding) {
      const overlay = el.querySelector('.el-loading-mask') as HTMLElement | null
      if (!overlay) return
      overlay.style.display = binding.value ? 'flex' : 'none'
    }
  }

  app.directive('tooltip', Tooltip)
  app.directive('loading', loadingDirective)

  app.component('el-button', ElButton)
  app.component('el-card', ElCard)
  app.component('el-tag', ElTag)
  app.component('el-progress', ElProgress)
  app.component('el-alert', ElAlert)
  app.component('el-divider', Divider)
  app.component('el-input', InputText)
  app.component('el-input-number', InputNumber)
  app.component('el-switch', InputSwitch)
  app.component('el-checkbox', ElCheckbox)
  app.component('el-checkbox-group', ElCheckboxGroup)
  app.component('el-radio', ElRadio)
  app.component('el-radio-group', ElRadioGroup)
  app.component('el-radio-button', ElRadio)
  app.component('el-slider', Slider)
  app.component('el-select', ElSelect)
  app.component('el-option', ElOption)
  app.component('el-dialog', ElDialog)
  app.component('el-table', ElTable)
  app.component('el-table-column', ElTableColumn)
  app.component('el-tabs', ElTabs)
  app.component('el-tab-pane', ElTabPane)
  app.component('el-pagination', ElPagination)
  app.component('el-row', ElRow)
  app.component('el-col', ElCol)
  app.component('el-tooltip', ElTooltip)
  app.component('el-empty', ElEmpty)
  app.component('el-result', ElResult)
  app.component('el-steps', ElSteps)
  app.component('el-step', ElStep)
  app.component('el-timeline', ElTimeline)
  app.component('el-timeline-item', ElTimelineItem)
  app.component('el-collapse', ElCollapse)
  app.component('el-collapse-item', ElCollapseItem)
  app.component('el-badge', Badge)
  app.component('el-drawer', Drawer)
  app.component('el-avatar', ElAvatar)
  app.component('el-skeleton', Skeleton)
  app.component('el-upload', FileUpload)
  app.component('el-icon', ElIcon)
  app.component('el-menu', ElMenu)
  app.component('el-menu-item', ElMenuItem)
  app.component('el-sub-menu', ElSubMenu)
  app.component('el-form', ElForm)
  app.component('el-form-item', ElFormItem)
  app.component('el-dropdown', ElDropdown)
  app.component('el-dropdown-menu', ElDropdownMenu)
  app.component('el-dropdown-item', ElDropdownItem)
  app.component('el-text', ElText)
  app.component('el-link', ElLink)
  app.component('el-descriptions', ElDescriptions)
  app.component('el-descriptions-item', ElDescriptionsItem)
  app.component('el-breadcrumb', ElBreadcrumb)
  app.component('el-breadcrumb-item', ElBreadcrumbItem)
  app.component('el-backtop', ElBacktop)
  app.component('el-page-header', ElPageHeader)
  app.component('el-statistic', ElStatistic)
  app.component('el-segmented', ElSegmented)
  app.component('el-scrollbar', ElScrollbar)
  app.component('el-date-picker', ElDatePicker)
  app.component('el-popconfirm', ElPopconfirm)
  app.component('el-button-group', ElButtonGroup)
}
