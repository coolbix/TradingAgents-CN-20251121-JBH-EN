import { defineComponent, h } from 'vue'

const createIcon = (name: string, classes: string) =>
  defineComponent({
    name,
    setup() {
      return () => h('i', { class: classes })
    }
  })

export const ArrowLeft = createIcon('ArrowLeft', 'pi pi-arrow-left')
export const ArrowDown = createIcon('ArrowDown', 'pi pi-arrow-down')
export const ArrowRight = createIcon('ArrowRight', 'pi pi-arrow-right')
export const Check = createIcon('Check', 'pi pi-check')
export const CircleCheck = createIcon('CircleCheck', 'pi pi-check-circle')
export const ChatDotRound = createIcon('ChatDotRound', 'pi pi-comments')
export const CircleClose = createIcon('CircleClose', 'pi pi-times-circle')
export const CircleCloseFilled = createIcon('CircleCloseFilled', 'pi pi-times-circle')
export const Clock = createIcon('Clock', 'pi pi-clock')
export const Close = createIcon('Close', 'pi pi-times')
export const Coin = createIcon('Coin', 'pi pi-bitcoin')
export const Collection = createIcon('Collection', 'pi pi-folder')
export const Connection = createIcon('Connection', 'pi pi-link')
export const CreditCard = createIcon('CreditCard', 'pi pi-credit-card')
export const Cpu = createIcon('Cpu', 'pi pi-microchip')
export const DataAnalysis = createIcon('DataAnalysis', 'pi pi-chart-line')
export const DataBoard = createIcon('DataBoard', 'pi pi-chart-bar')
export const Bell = createIcon('Bell', 'pi pi-bell')
export const Brush = createIcon('Brush', 'pi pi-palette')
export const Delete = createIcon('Delete', 'pi pi-trash')
export const Document = createIcon('Document', 'pi pi-file')
export const Download = createIcon('Download', 'pi pi-download')
export const Edit = createIcon('Edit', 'pi pi-pencil')
export const Expand = createIcon('Expand', 'pi pi-arrows-alt')
export const Files = createIcon('Files', 'pi pi-copy')
export const FullScreen = createIcon('FullScreen', 'pi pi-window-maximize')
export const Fold = createIcon('Fold', 'pi pi-chevron-left')
export const InfoFilled = createIcon('InfoFilled', 'pi pi-info-circle')
export const Key = createIcon('Key', 'pi pi-key')
export const Link = createIcon('Link', 'pi pi-link')
export const Lock = createIcon('Lock', 'pi pi-lock')
export const List = createIcon('List', 'pi pi-list')
export const Loading = createIcon('Loading', 'pi pi-spin pi pi-spinner')
export const Money = createIcon('Money', 'pi pi-dollar')
export const Message = createIcon('Message', 'pi pi-envelope')
export const Monitor = createIcon('Monitor', 'pi pi-desktop')
export const Moon = createIcon('Moon', 'pi pi-moon')
export const Odometer = createIcon('Odometer', 'pi pi-gauge')
export const Operation = createIcon('Operation', 'pi pi-cog')
export const OfficeBuilding = createIcon('OfficeBuilding', 'pi pi-building')
export const Plus = createIcon('Plus', 'pi pi-plus')
export const Promotion = createIcon('Promotion', 'pi pi-send')
export const QuestionFilled = createIcon('QuestionFilled', 'pi pi-question-circle')
export const Rank = createIcon('Rank', 'pi pi-sort-amount-up')
export const Reading = createIcon('Reading', 'pi pi-book')
export const Refresh = createIcon('Refresh', 'pi pi-refresh')
export const RefreshRight = createIcon('RefreshRight', 'pi pi-refresh')
export const Search = createIcon('Search', 'pi pi-search')
export const Select = createIcon('Select', 'pi pi-check')
export const Setting = createIcon('Setting', 'pi pi-cog')
export const Star = createIcon('Star', 'pi pi-star')
export const SuccessFilled = createIcon('SuccessFilled', 'pi pi-check-circle')
export const Sunny = createIcon('Sunny', 'pi pi-sun')
export const SwitchButton = createIcon('SwitchButton', 'pi pi-sign-out')
export const Tools = createIcon('Tools', 'pi pi-wrench')
export const TrendCharts = createIcon('TrendCharts', 'pi pi-chart-line')
export const Timer = createIcon('Timer', 'pi pi-clock')
export const Upload = createIcon('Upload', 'pi pi-upload')
export const User = createIcon('User', 'pi pi-user')
export const View = createIcon('View', 'pi pi-eye')
export const VideoPlay = createIcon('VideoPlay', 'pi pi-play')
export const VideoPause = createIcon('VideoPause', 'pi pi-pause')
export const Warning = createIcon('Warning', 'pi pi-exclamation-triangle')
export const WarningFilled = createIcon('WarningFilled', 'pi pi-exclamation-triangle')

export default {
  ArrowLeft,
  ArrowDown,
  ArrowRight,
  Check,
  CircleCheck,
  ChatDotRound,
  CircleClose,
  CircleCloseFilled,
  Clock,
  Close,
  Coin,
  Collection,
  Connection,
  CreditCard,
  Cpu,
  DataAnalysis,
  DataBoard,
  Bell,
  Brush,
  Delete,
  Document,
  Download,
  Edit,
  Expand,
  Files,
  FullScreen,
  Fold,
  InfoFilled,
  Key,
  Link,
  Lock,
  List,
  Loading,
  Money,
  Message,
  Monitor,
  Moon,
  Odometer,
  Operation,
  OfficeBuilding,
  Plus,
  Promotion,
  QuestionFilled,
  Rank,
  Reading,
  Refresh,
  RefreshRight,
  Search,
  Select,
  Setting,
  Star,
  SuccessFilled,
  Sunny,
  SwitchButton,
  Tools,
  TrendCharts,
  Timer,
  Upload,
  User,
  View,
  VideoPlay,
  VideoPause,
  Warning,
  WarningFilled
}
