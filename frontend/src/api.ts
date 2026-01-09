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
    error_type?: string | number
    description?: string
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

export interface AlarmsResponse {
    adjustments: AlarmAdjustment[]
    total: number
    page: number
    page_size: number
    total_pages: number
}

export interface FileItem {
    name: string
    type: 'directory' | 'file'
    size: number
    mtime: string
    mime_type: string | null
    path: string
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

export interface AlarmsFilters {
    alarm_code?: string
    station_nr?: string
    sort_by?: string
    sort_order?: 'asc' | 'desc'
}

export const getAlarms = async (page: number = 1, pageSize: number = 10, filters: AlarmsFilters = {}): Promise<AlarmsResponse> => {
    let url = `/alarms?page=${page}&page_size=${pageSize}`

    if (filters.alarm_code) url += `&alarm_code=${filters.alarm_code}`
    if (filters.station_nr) url += `&station_nr=${filters.station_nr}`
    if (filters.sort_by) url += `&sort_by=${filters.sort_by}`
    if (filters.sort_order) url += `&sort_order=${filters.sort_order}`

    const response = await api.get(url)
    return response.data
}

export const getSourceAlarms = async (
    period: string,
    station_nr?: number,
    alarm_code?: number,
    error_type?: 'stopping' | 'non_stopping'
): Promise<AlarmAdjustment[]> => {
    let url = `/alarms/source?period=${period}`
    if (station_nr) url += `&station_nr=${station_nr}`
    if (alarm_code) url += `&alarm_code=${alarm_code}`
    if (error_type) url += `&error_type=${error_type}`

    const response = await api.get(url)
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

export const getAlarmIds = async (filters: AlarmsFilters = {}): Promise<number[]> => {
    let url = '/alarms/ids'
    const params = new URLSearchParams()
    if (filters.alarm_code) params.append('alarm_code', filters.alarm_code)
    if (filters.station_nr) params.append('station_nr', filters.station_nr)
    // Add timestamp to prevent caching issues with proxy
    params.append('_t', Date.now().toString())
    if (params.toString()) url += `?${params.toString()}`

    const response = await api.get(url)
    return response.data
}

export const bulkDeleteAlarms = async (ids: number[]) => {
    const response = await api.post('/alarms/bulk/delete', { ids })
    return response.data
}

export const bulkUpdateAlarms = async (ids: number[], data: Partial<AlarmAdjustment>) => {
    // Only send updatable fields. Filter out undefined values.
    const updateData: Record<string, string | undefined> = {}
    if (data.time_on !== undefined) updateData.time_on = data.time_on
    if (data.time_off !== undefined) updateData.time_off = data.time_off
    if (data.notes !== undefined) updateData.notes = data.notes

    const response = await api.put('/alarms/bulk/update', { ids, data: updateData })
    return response.data
}

export const bulkUpsertAlarms = async (adjustments: AlarmAdjustment[]) => {
    const response = await api.post('/alarms/bulk/upsert', { adjustments })
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

export const listFiles = async (path: string = ''): Promise<FileItem[]> => {
    const response = await api.get('/fs/list', { params: { path } })
    return response.data
}

export const getDownloadUrl = (path: string) => {
    // Construct absolute URL for direct browser navigation
    // Note: In development with Vite proxy, this works. In production, ensure paths match.
    return `${API_BASE_URL}/fs/download?path=${encodeURIComponent(path)}`
}

export const downloadZip = async (paths: string[]) => {
    const response = await api.post('/fs/download-zip', { paths }, {
        responseType: 'blob', // Important for binary data
    })

    // Create blob link to download
    const url = window.URL.createObjectURL(new Blob([response.data]))
    const link = document.createElement('a')
    link.href = url
    link.setAttribute('download', 'files.zip')
    document.body.appendChild(link)
    link.click()
    link.parentNode?.removeChild(link)
    window.URL.revokeObjectURL(url)
}

export interface SearchParams {
    query?: string
    months?: string[]
    types?: string[]
}

export const searchFiles = async (params: SearchParams): Promise<FileItem[]> => {
    const response = await api.get('/fs/search', {
        params,
        paramsSerializer: {
            indexes: null
        }
    })
    return response.data
}

// Validation API
export interface ValidationIssue {
    type: string
    station_id?: number | string
    sensor?: string
    column?: string
    count: number
    range_start: string
    range_end: string
    sample_value?: any
    bounds?: [number, number]
    // Completeness fields
    completeness_pct?: number
    total_expected?: number
    missing_timestamps?: string[]
}

export interface FileValidationReport {
    file: string
    issues: ValidationIssue[]
}

export interface ValidationReport {
    last_run: string | null
    summary: {
        total_files_scanned: number
        total_issues: number
        files_with_issues: number
        stuck_values_count: number
        out_of_range_count: number
        completeness_issues_count?: number
        system_issues_count?: number
        empty_rows_count?: number
        sensor_gaps_count?: number
    }
    details: FileValidationReport[]
}


export interface ValidationRequest {
    dates?: string[]
    start_date?: string
    end_date?: string
    stuck_intervals?: number
    exclude_zero?: boolean
}

export const runValidation = async (request: ValidationRequest = {}) => {
    const response = await api.post('/integrity/run', request)
    return response.data
}

export const getValidationReport = async (): Promise<ValidationReport> => {
    const response = await api.get('/integrity/report')
    return response.data
}

export interface IntegrityRules {
    ranges: Record<string, [number, number]>
    defaults: {
        stuck_intervals: number
    }
}

export const getValidationRules = async (): Promise<IntegrityRules> => {
    const response = await api.get('/integrity/rules')
    return response.data
}

// Scheduler API
export interface SchedulerStatus {
    enabled: boolean
    day_of_week: string
    hour: number
    minute: number
    next_run: string | null
    last_run: string | null
    last_status: 'success' | 'error' | null
    last_error: string | null
    is_running: boolean
}

export interface SchedulerConfigRequest {
    enabled: boolean
    day_of_week: string
    hour: number
    minute: number
}

export const getSchedulerStatus = async (): Promise<SchedulerStatus> => {
    const response = await api.get('/scheduler/status')
    return response.data
}

export const configureScheduler = async (config: SchedulerConfigRequest): Promise<SchedulerStatus> => {
    const response = await api.post('/scheduler/configure', config)
    return response.data
}

export const triggerScheduler = async () => {
    const response = await api.post('/scheduler/trigger')
    return response.data
}

// Connection Test API
export interface TestResult {
    success: boolean
    message: string
    details: Record<string, string | number>
}

export const testDatabaseConnection = async (): Promise<TestResult> => {
    const response = await api.post('/test/database')
    return response.data
}

export const testEmailConfiguration = async (): Promise<TestResult> => {
    const response = await api.post('/test/email')
    return response.data
}

// App Settings API
export interface AppSettings {
    email_enabled: boolean
    default_update_mode: string
    calculation_source: string
}

export const getAppSettings = async (): Promise<AppSettings> => {
    const response = await api.get('/settings')
    return response.data
}

export const updateAppSettings = async (settings: AppSettings): Promise<AppSettings> => {
    const response = await api.post('/settings', settings)
    return response.data
}
