import type { ToastServiceMethods } from 'primevue/toastservice'
import type { ConfirmationServiceMethods } from 'primevue/confirmationservice'

let toastService: ToastServiceMethods | null = null
let confirmService: ConfirmationServiceMethods | null = null

export const setPrimeToastService = (service: ToastServiceMethods | null) => {
  toastService = service
}

export const setPrimeConfirmService = (service: ConfirmationServiceMethods | null) => {
  confirmService = service
}

export const getPrimeToastService = () => toastService
export const getPrimeConfirmService = () => confirmService
