import axios from 'axios'

const API_BASE_URL = '/api'

export const api = axios.create({
    baseURL: API_BASE_URL,
    headers: {
        'Content-Type': 'application/json',
    },
})

// Types
export interface ProcessRequest {
    dates: string[]
    update_mode: 'append' | 'check' | 'force-overwrite' | 'process-existing'
}

export interface ProcessingStatus {
    status: 'idle' | 'starting' | 'running' | 'completed' | 'error'
    message: string
    date?: string
    step?: string
}

export interface AlarmAdjustment {
    id: number
    alarm_code: number
    station_nr: number
    time_on?: string
    time_off?: string
    notes?: string
    last_updated?: string
}

export interface SystemStatus {
    overall: 'healthy' | 'degraded'
    components: {
        component: string
        status: 'ok' | 'warning' | 'error'
        details: string
    }[]
    processing: ProcessingStatus
}

// API Functions
export const processData = async (request: ProcessRequest) => {
    const response = await api.post('/process', request)
    return response.data
}

export const abortProcessing = async () => {
    const response = await api.post('/process/abort')
    return response.data
}

export const getProcessingStatus = async (): Promise<ProcessingStatus> => {
    const response = await api.get('/process/status')
    return response.data
}

export const getAlarms = async (): Promise<{ adjustments: AlarmAdjustment[] }> => {
    const response = await api.get('/alarms')
    return response.data
}

export const addAlarm = async (adjustment: AlarmAdjustment) => {
    const response = await api.post('/alarms', adjustment)
    return response.data
}

export const updateAlarm = async (id: number, data: Partial<AlarmAdjustment>) => {
    const response = await api.put(`/alarms/${id}`, data)
    return response.data
}

export const deleteAlarm = async (id: number) => {
    const response = await api.delete(`/alarms/${id}`)
    return response.data
}

export const getLogs = async (lines: number = 50): Promise<{ logs: string[], total_lines: number }> => {
    const response = await api.get(`/logs?lines=${lines}`)
    return response.data
}

export const getSystemStatus = async (): Promise<SystemStatus> => {
    const response = await api.get('/status')
    return response.data
}
