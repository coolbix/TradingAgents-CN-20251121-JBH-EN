import { defineComponent, h } from 'vue'

const createIcon = (name: string, classes: string) =>
  defineComponent({
    name,
    setup() {
      return () => h('i', { class: classes })
    }
  })

export const ArrowLeft = createIcon('ArrowLeft', 'pi pi-arrow-left')
export const ArrowRight = createIcon('ArrowRight', 'pi pi-arrow-right')
export const Check = createIcon('Check', 'pi pi-check')
export const CircleCheck = createIcon('CircleCheck', 'pi pi-check-circle')
export const CircleCloseFilled = createIcon('CircleCloseFilled', 'pi pi-times-circle')
export const Clock = createIcon('Clock', 'pi pi-clock')
export const Close = createIcon('Close', 'pi pi-times')
export const Collection = createIcon('Collection', 'pi pi-folder')
export const Connection = createIcon('Connection', 'pi pi-link')
export const CreditCard = createIcon('CreditCard', 'pi pi-credit-card')
export const DataAnalysis = createIcon('DataAnalysis', 'pi pi-chart-line')
export const Delete = createIcon('Delete', 'pi pi-trash')
export const Document = createIcon('Document', 'pi pi-file')
export const Download = createIcon('Download', 'pi pi-download')
export const Expand = createIcon('Expand', 'pi pi-arrows-alt')
export const Files = createIcon('Files', 'pi pi-copy')
export const Fold = createIcon('Fold', 'pi pi-chevron-left')
export const InfoFilled = createIcon('InfoFilled', 'pi pi-info-circle')
export const Link = createIcon('Link', 'pi pi-link')
export const List = createIcon('List', 'pi pi-list')
export const Loading = createIcon('Loading', 'pi pi-spin pi pi-spinner')
export const Money = createIcon('Money', 'pi pi-dollar')
export const Operation = createIcon('Operation', 'pi pi-cog')
export const Plus = createIcon('Plus', 'pi pi-plus')
export const Rank = createIcon('Rank', 'pi pi-sort-amount-up')
export const Reading = createIcon('Reading', 'pi pi-book')
export const Refresh = createIcon('Refresh', 'pi pi-refresh')
export const RefreshRight = createIcon('RefreshRight', 'pi pi-refresh')
export const Search = createIcon('Search', 'pi pi-search')
export const Setting = createIcon('Setting', 'pi pi-cog')
export const Star = createIcon('Star', 'pi pi-star')
export const SuccessFilled = createIcon('SuccessFilled', 'pi pi-check-circle')
export const SwitchButton = createIcon('SwitchButton', 'pi pi-sign-out')
export const TrendCharts = createIcon('TrendCharts', 'pi pi-chart-line')
export const Upload = createIcon('Upload', 'pi pi-upload')
export const User = createIcon('User', 'pi pi-user')
export const View = createIcon('View', 'pi pi-eye')
export const Warning = createIcon('Warning', 'pi pi-exclamation-triangle')

export default {
  ArrowLeft,
  ArrowRight,
  Check,
  CircleCheck,
  CircleCloseFilled,
  Clock,
  Close,
  Collection,
  Connection,
  CreditCard,
  DataAnalysis,
  Delete,
  Document,
  Download,
  Expand,
  Files,
  Fold,
  InfoFilled,
  Link,
  List,
  Loading,
  Money,
  Operation,
  Plus,
  Rank,
  Reading,
  Refresh,
  RefreshRight,
  Search,
  Setting,
  Star,
  SuccessFilled,
  SwitchButton,
  TrendCharts,
  Upload,
  User,
  View,
  Warning
}
