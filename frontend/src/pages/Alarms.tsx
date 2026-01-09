import { useState } from 'react'
import {
    Container,
    Title,
    Text,
    Card,
    Button,
    Group,
    Stack,
    Modal,
    TextInput,
    NumberInput,
    ActionIcon,
    Badge,
    Tooltip,
    Checkbox,
} from '@mantine/core'
import { useDisclosure } from '@mantine/hooks'
import { notifications } from '@mantine/notifications'
import { useQuery, useMutation, useQueryClient, keepPreviousData } from '@tanstack/react-query'
import {
    IconPlus,
    IconEdit,
    IconTrash,
    IconCheck,
    IconX,
} from '@tabler/icons-react'
import dayjs from 'dayjs'
import { DataTable, type DataTableSortStatus } from 'mantine-datatable'

import { bulkUpsertAlarms, getAlarms, addAlarm, updateAlarm, deleteAlarm, bulkDeleteAlarms, bulkUpdateAlarms, getAlarmIds, getSourceAlarms } from '../api'
import type { AlarmAdjustment } from '../api'
import { DateInput } from '@mantine/dates'
import {
    Tabs,
    Select,
    Alert
} from '@mantine/core'
import { IconSearch, IconAlertCircle } from '@tabler/icons-react'
import SourceAlarmTable from '../components/SourceAlarmTable'
import { type RowSelectionState } from '@tanstack/react-table'

export default function Alarms() {
    const queryClient = useQueryClient()
    const [opened, { open, close }] = useDisclosure(false)
    const [activeTab, setActiveTab] = useState<string | null>('guided')
    const [editingAlarm, setEditingAlarm] = useState<AlarmAdjustment | null>(null)
    const [formData, setFormData] = useState({
        id: 0,
        alarm_code: 0,
        station_nr: 0,
        time_on: '',
        time_off: '',
        notes: '',
    })

    // Bulk Actions State
    const [selectedRecords, setSelectedRecords] = useState<AlarmAdjustment[]>([])
    const [selectAllPages, setSelectAllPages] = useState(false)
    const [bulkEditOpened, { open: openBulkEdit, close: closeBulkEdit }] = useDisclosure(false)
    const [bulkFormData, setBulkFormData] = useState({
        updateTimeOn: false,
        time_on: '',
        updateTimeOff: false,
        time_off: '',
        updateNotes: false,
        notes: '',
    })

    // Guided Mode State
    const [searchPeriod, setSearchPeriod] = useState<Date | null>(new Date())
    const [searchStation, setSearchStation] = useState<number | ''>('')
    const [searchCode, setSearchCode] = useState<number | ''>('')
    const [searchType, setSearchType] = useState<string>('all')
    const [sourceAlarms, setSourceAlarms] = useState<AlarmAdjustment[]>([])
    const [isSearching, setIsSearching] = useState(false)
    const [sourceRowSelection, setSourceRowSelection] = useState<RowSelectionState>({})

    const [page, setPage] = useState(1)
    const PAGE_SIZE = 10

    const [filters, setFilters] = useState({
        alarm_code: '',
        station_nr: '',
    })

    const [sortStatus, setSortStatus] = useState<DataTableSortStatus<AlarmAdjustment>>({
        columnAccessor: 'id',
        direction: 'desc',
    })

    const { data: alarmsData, isLoading, isFetching } = useQuery({
        queryKey: ['alarms', page, filters, sortStatus],
        queryFn: () => getAlarms(page, PAGE_SIZE, {
            alarm_code: filters.alarm_code || undefined,
            station_nr: filters.station_nr || undefined,
            sort_by: sortStatus.columnAccessor,
            sort_order: sortStatus.direction,
        }),
        placeholderData: keepPreviousData,
    })

    const addMutation = useMutation({
        mutationFn: addAlarm,
        onSuccess: () => {
            notifications.show({
                title: 'Success',
                message: 'Alarm adjustment added',
                color: 'green',
                icon: <IconCheck size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['alarms'] })
            close()
            resetForm()
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
                icon: <IconX size={16} />,
            })
        },
    })

    const updateMutation = useMutation({
        mutationFn: ({ id, data }: { id: number; data: Partial<AlarmAdjustment> }) =>
            updateAlarm(id, data),
        onSuccess: () => {
            notifications.show({
                title: 'Success',
                message: 'Alarm adjustment updated',
                color: 'green',
                icon: <IconCheck size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['alarms'] })
            close()
            resetForm()
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
                icon: <IconX size={16} />,
            })
        },
    })

    const deleteMutation = useMutation({
        mutationFn: deleteAlarm,
        onSuccess: () => {
            notifications.show({
                title: 'Success',
                message: 'Alarm adjustment deleted',
                color: 'green',
                icon: <IconCheck size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['alarms'] })
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
                icon: <IconX size={16} />,
            })
        },
    })

    const bulkDeleteMutation = useMutation({
        mutationFn: bulkDeleteAlarms,
        onSuccess: (data) => {
            notifications.show({
                title: 'Success',
                message: data.message || 'Selected adjustments deleted',
                color: 'green',
                icon: <IconCheck size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['alarms'] })
            setSelectedRecords([])
            setSelectAllPages(false)
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
                icon: <IconX size={16} />,
            })
        },
    })

    const bulkUpdateMutation = useMutation({
        mutationFn: ({ ids, data }: { ids: number[]; data: Partial<AlarmAdjustment> }) =>
            bulkUpdateAlarms(ids, data),
        onSuccess: (data) => {
            notifications.show({
                title: 'Success',
                message: data.message || 'Selected adjustments updated',
                color: 'green',
                icon: <IconCheck size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['alarms'] })
            closeBulkEdit()
            setSelectedRecords([])
            setSelectAllPages(false)
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
                icon: <IconX size={16} />,
            })
        },
    })

    const resetForm = () => {
        setFormData({
            id: 0,
            alarm_code: 0,
            station_nr: 0,
            time_on: '',
            time_off: '',
            notes: '',
        })
        setEditingAlarm(null)
    }

    const handleSearch = async () => {
        if (!searchPeriod) return

        setIsSearching(true)
        try {
            // Use dayjs for robust date formatting (handles Date objects and other formats safely)
            const periodStr = dayjs(searchPeriod).format('YYYY-MM')
            const results = await getSourceAlarms(
                periodStr,
                searchStation !== '' ? Number(searchStation) : undefined,
                searchCode !== '' ? Number(searchCode) : undefined,
                searchType !== 'all' ? searchType as 'stopping' | 'non_stopping' : undefined
            )
            setSourceAlarms(results)
            if (results.length === 0) {
                notifications.show({ message: 'No alarms found matching criteria', color: 'yellow' })
            }
        } catch (error: any) {
            notifications.show({ title: 'Search Failed', message: error.message, color: 'red' })
        } finally {
            setIsSearching(false)
        }
    }



    // Opens the modal for a source alarm (Guided Mode)
    const handleAdjustSourceAlarm = (alarm: AlarmAdjustment) => {
        // Check if an adjustment already exists for this ID
        const existingAdj = alarmsData?.adjustments.find(a => a.id === alarm.id)
        if (existingAdj) {
            handleOpenEdit(existingAdj)
        } else {
            // New adjustment for this alarm
            setEditingAlarm(null) // It's a "new" entry in customizations, even if ID is same
            setFormData({
                id: alarm.id,
                alarm_code: alarm.alarm_code,
                station_nr: alarm.station_nr,
                time_on: alarm.time_on ? dayjs(alarm.time_on).format('YYYY-MM-DD HH:mm:ss') : '',
                time_off: alarm.time_off ? dayjs(alarm.time_off).format('YYYY-MM-DD HH:mm:ss') : '',
                notes: '', // Notes start empty
            })
            open()
        }
    }

    const handleOpenAdd = () => {
        resetForm()
        open()
    }

    const handleOpenEdit = (alarm: AlarmAdjustment) => {
        setEditingAlarm(alarm)
        setFormData({
            id: alarm.id,
            alarm_code: alarm.alarm_code,
            station_nr: alarm.station_nr,
            time_on: alarm.time_on || '',
            time_off: alarm.time_off || '',
            notes: alarm.notes || '',
        })
        open()
    }

    const handleSubmit = () => {
        if (!formData.time_on && !formData.time_off) {
            notifications.show({
                title: 'Validation Error',
                message: 'At least one of Time On or Time Off must be provided',
                color: 'yellow',
            })
            return
        }

        if (editingAlarm) {
            updateMutation.mutate({
                id: formData.id,
                data: {
                    time_on: formData.time_on || undefined,
                    time_off: formData.time_off || undefined,
                    notes: formData.notes || undefined,
                },
            })
        } else {
            addMutation.mutate(formData as AlarmAdjustment)
        }
    }

    const handleDelete = (id: number) => {
        if (window.confirm('Are you sure you want to delete this adjustment?')) {
            deleteMutation.mutate(id)
        }
    }

    const handleBulkDelete = async () => {
        if (!window.confirm(`Are you sure you want to delete ${selectAllPages ? alarmsData?.total : selectedRecords.length} adjustments?`)) {
            return
        }

        let ids: number[] = []
        if (selectAllPages) {
            ids = await getAlarmIds({
                alarm_code: filters.alarm_code || undefined,
                station_nr: filters.station_nr || undefined,
            })
        } else {
            ids = selectedRecords.map(r => r.id)
        }

        bulkDeleteMutation.mutate(ids)
    }

    const handleBulkSubmit = async () => {
        // Validation first
        if (!bulkFormData.updateTimeOn && !bulkFormData.updateTimeOff && !bulkFormData.updateNotes) {
            notifications.show({
                title: 'Validation Error',
                message: 'No fields selected for update',
                color: 'yellow',
            })
            return
        }

        const data: Partial<AlarmAdjustment> = {}
        if (bulkFormData.updateTimeOn) data.time_on = bulkFormData.time_on
        if (bulkFormData.updateTimeOff) data.time_off = bulkFormData.time_off
        if (bulkFormData.updateNotes) data.notes = bulkFormData.notes

        try {
            if (activeTab === 'guided') {
                // Guided Mode Bulk Upsert
                const selectedIds = Object.keys(sourceRowSelection).map(Number)
                if (selectedIds.length === 0) return

                // Get full alarm objects for selected IDs
                const selectedAlarms = sourceAlarms.filter(a => selectedIds.includes(a.id))

                // Merge form data into each alarm object
                const adjustmentsToUpsert = selectedAlarms.map(a => ({
                    id: a.id,
                    alarm_code: a.alarm_code,
                    station_nr: a.station_nr,
                    ...data
                }))

                await bulkUpsertAlarms(adjustmentsToUpsert)

                notifications.show({
                    title: 'Success',
                    message: `Successfully adjusted ${selectedIds.length} alarms`,
                    color: 'green',
                    icon: <IconCheck size={16} />,
                })

                queryClient.invalidateQueries({ queryKey: ['alarms'] })
                closeBulkEdit()
                setSourceRowSelection({})

            } else {
                // Existing Adjustments Mode (Update Only)
                let ids: number[] = []
                if (selectAllPages) {
                    ids = await getAlarmIds({
                        alarm_code: filters.alarm_code || undefined,
                        station_nr: filters.station_nr || undefined,
                    })
                } else {
                    ids = selectedRecords.map(r => r.id)
                }

                bulkUpdateMutation.mutate({ ids, data })
            }
        } catch (error: any) {
            notifications.show({
                title: 'Error',
                message: error.message || 'Bulk action failed',
                color: 'red',
                icon: <IconX size={16} />,
            })
        }
    }

    const toggleSelectAllPages = () => {
        if (selectAllPages) {
            setSelectAllPages(false)
            setSelectedRecords([])
        } else {
            setSelectAllPages(true)
            // When selecting all pages, we visually select all on current page too
            setSelectedRecords(adjustments)
        }
    }

    const adjustments = alarmsData?.adjustments || []

    return (
        <Container size="lg">
            <Stack gap="xl">
                {/* Page Title */}
                <Group justify="space-between">
                    <div>
                        <Title order={2} mb={4}>Alarm Adjustments</Title>
                        <Text c="dimmed">Manage manual alarm time adjustments</Text>
                    </div>
                    <Button leftSection={<IconPlus size={16} />} onClick={handleOpenAdd}>
                        Add Adjustment
                    </Button>
                </Group>



                <Tabs value={activeTab} onChange={setActiveTab}>
                    <Tabs.List mb="md">
                        <Tabs.Tab value="guided" leftSection={<IconSearch size={16} />}>
                            Guided Mode
                        </Tabs.Tab>
                        <Tabs.Tab value="existing" leftSection={<IconEdit size={16} />}>
                            Existing Adjustments
                        </Tabs.Tab>
                    </Tabs.List>

                    <Tabs.Panel value="guided">
                        <Stack gap="md">
                            <Alert icon={<IconAlertCircle size={16} />} color="blue" variant="light">
                                Search for alarms in the source data to easily apply adjustments.
                            </Alert>

                            <Group align="flex-end">
                                <DateInput
                                    label="Month"
                                    placeholder="Select month"
                                    value={searchPeriod}
                                    onChange={(val: any) => setSearchPeriod(val)}
                                    valueFormat="YYYY-MM"
                                    maxLevel="year"
                                />
                                <NumberInput
                                    label="Station Nr"
                                    placeholder="e.g. 2307405"
                                    value={searchStation}
                                    onChange={(val) => setSearchStation(val === '' ? '' : Number(val))}
                                    min={0}
                                />
                                <NumberInput
                                    label="Alarm Code"
                                    placeholder="e.g. 52"
                                    value={searchCode}
                                    onChange={(val) => setSearchCode(val === '' ? '' : Number(val))}
                                    min={0}
                                />
                                <Select
                                    label="Type"
                                    data={[
                                        { value: 'all', label: 'All Types' },
                                        { value: 'stopping', label: 'Stopping (0/1)' },
                                        { value: 'non_stopping', label: 'Non-Stopping' },
                                    ]}
                                    value={searchType}
                                    onChange={(val) => setSearchType(val || 'all')}
                                />
                                <Button
                                    leftSection={<IconSearch size={16} />}
                                    loading={isSearching}
                                    onClick={handleSearch}
                                    disabled={!searchPeriod}
                                >
                                    Search
                                </Button>
                            </Group>

                            {/* Guided Mode Bulk Actions Banner */}
                            {Object.keys(sourceRowSelection).length > 0 && (
                                <Card withBorder padding="sm" radius="md" bg="var(--mantine-color-blue-light)">
                                    <Group justify="space-between">
                                        <Group>
                                            <Text size="sm" fw={500}>
                                                {Object.keys(sourceRowSelection).length} alarms selected.
                                            </Text>
                                        </Group>
                                        <Group>
                                            <Button
                                                variant="white"
                                                size="xs"
                                                leftSection={<IconEdit size={14} />}
                                                onClick={openBulkEdit}
                                            >
                                                Bulk Adjust
                                            </Button>
                                        </Group>
                                    </Group>
                                </Card>
                            )}

                            {sourceAlarms.length > 0 && (
                                <SourceAlarmTable
                                    data={sourceAlarms}
                                    onAdjust={handleAdjustSourceAlarm}
                                    rowSelection={sourceRowSelection}
                                    setRowSelection={setSourceRowSelection}
                                />
                            )}
                            {sourceAlarms.length === 0 && !isSearching && searchPeriod && (
                                <Text c="dimmed" ta="center" py="xl">
                                    No alarms found. Try adjusting filters and searching.
                                </Text>
                            )}
                        </Stack>
                    </Tabs.Panel>

                    <Tabs.Panel value="existing">
                        <Stack gap="xl">
                            {/* Bulk Actions Banner */}
                            {selectedRecords.length > 0 && (
                                <Card withBorder padding="sm" radius="md" bg="var(--mantine-color-blue-light)">
                                    <Group justify="space-between">
                                        <Group>
                                            <Checkbox
                                                checked={true}
                                                indeterminate={!selectAllPages && adjustments.length < (alarmsData?.total || 0) && selectedRecords.length < (alarmsData?.total || 0)}
                                                onChange={() => {
                                                    setSelectedRecords([])
                                                    setSelectAllPages(false)
                                                }}
                                            />
                                            <Text size="sm" fw={500}>
                                                {selectAllPages
                                                    ? `All ${alarmsData?.total} adjustments are selected.`
                                                    : `${selectedRecords.length} selected.`}
                                            </Text>
                                            {!selectAllPages && (alarmsData?.total || 0) > adjustments.length && selectedRecords.length === adjustments.length && (
                                                <Button variant="subtle" size="compact-sm" onClick={toggleSelectAllPages}>
                                                    Select all {alarmsData?.total} adjustments across all pages
                                                </Button>
                                            )}
                                        </Group>
                                        <Group>
                                            <Button
                                                variant="white"
                                                size="xs"
                                                leftSection={<IconEdit size={14} />}
                                                onClick={openBulkEdit}
                                            >
                                                Bulk Edit
                                            </Button>
                                            <Button
                                                variant="white"
                                                color="red"
                                                size="xs"
                                                leftSection={<IconTrash size={14} />}
                                                onClick={handleBulkDelete}
                                                loading={bulkDeleteMutation.isPending}
                                            >
                                                Delete Selected
                                            </Button>
                                        </Group>
                                    </Group>
                                </Card>
                            )}

                            {/* Alarms Table */}
                            <Card shadow="sm" padding={0} radius="md" withBorder>
                                <DataTable
                                    withTableBorder={false}
                                    borderRadius="md"
                                    minHeight={200}
                                    loaderColor="blue"
                                    fetching={isLoading || isFetching}
                                    records={adjustments}
                                    columns={[
                                        {
                                            accessor: 'id',
                                            title: 'ID',
                                            sortable: true,
                                            render: (record) => <Badge variant="light">{record.id}</Badge>,
                                        },
                                        {
                                            accessor: 'alarm_code',
                                            title: 'Alarm Code',
                                            sortable: true,
                                            filter: (
                                                <TextInput
                                                    size="xs"
                                                    placeholder="Filter..."
                                                    value={filters.alarm_code}
                                                    onChange={(e) => {
                                                        setFilters({ ...filters, alarm_code: e.target.value })
                                                        setPage(1)
                                                        setSelectedRecords([])
                                                        setSelectAllPages(false)
                                                    }}
                                                />
                                            ),
                                            filtering: filters.alarm_code !== '',
                                        },
                                        {
                                            accessor: 'station_nr',
                                            title: 'Station Nr',
                                            sortable: true,
                                            filter: (
                                                <TextInput
                                                    size="xs"
                                                    placeholder="Filter..."
                                                    value={filters.station_nr}
                                                    onChange={(e) => {
                                                        setFilters({ ...filters, station_nr: e.target.value })
                                                        setPage(1)
                                                        setSelectedRecords([])
                                                        setSelectAllPages(false)
                                                    }}
                                                />
                                            ),
                                            filtering: filters.station_nr !== '',
                                        },
                                        {
                                            accessor: 'time_on',
                                            title: 'Time On',
                                            sortable: true,
                                            render: (record) => record.time_on ? dayjs(record.time_on).format('YYYY-MM-DD HH:mm:ss') : <Text size="sm" c="dimmed">—</Text>,
                                        },
                                        {
                                            accessor: 'time_off',
                                            title: 'Time Off',
                                            sortable: true,
                                            render: (record) => record.time_off ? dayjs(record.time_off).format('YYYY-MM-DD HH:mm:ss') : <Text size="sm" c="dimmed">—</Text>,
                                        },
                                        {
                                            accessor: 'notes',
                                            title: 'Notes',
                                            render: (record) => <Text size="sm" lineClamp={1} maw={150}>{record.notes || '—'}</Text>,
                                        },
                                        {
                                            accessor: 'last_updated',
                                            title: 'Last Updated',
                                            sortable: true,
                                            render: (record) => record.last_updated ? <Text size="xs" c="dimmed">{dayjs(record.last_updated).format('YYYY-MM-DD HH:mm')}</Text> : '—',
                                        },
                                        {
                                            accessor: 'actions',
                                            title: 'Actions',
                                            textAlign: 'right',
                                            render: (record) => (
                                                <Group gap={4} justify="right">
                                                    <Tooltip label="Edit">
                                                        <ActionIcon
                                                            variant="subtle"
                                                            color="blue"
                                                            onClick={() => handleOpenEdit(record)}
                                                        >
                                                            <IconEdit size={16} />
                                                        </ActionIcon>
                                                    </Tooltip>
                                                    <Tooltip label="Delete">
                                                        <ActionIcon
                                                            variant="subtle"
                                                            color="red"
                                                            onClick={() => handleDelete(record.id)}
                                                            loading={deleteMutation.isPending}
                                                        >
                                                            <IconTrash size={16} />
                                                        </ActionIcon>
                                                    </Tooltip>
                                                </Group>
                                            ),
                                        },
                                    ]}
                                    totalRecords={alarmsData?.total || 0}
                                    recordsPerPage={PAGE_SIZE}
                                    page={page}
                                    onPageChange={setPage}
                                    sortStatus={sortStatus}
                                    onSortStatusChange={setSortStatus as any}
                                    selectedRecords={selectedRecords}
                                    onSelectedRecordsChange={(records) => {
                                        setSelectedRecords(records)
                                        // If user manually deselects something, we turn off "select all pages" mode
                                        if (selectAllPages && records.length < adjustments.length) {
                                            setSelectAllPages(false)
                                        }
                                    }}
                                />
                            </Card>
                        </Stack>
                    </Tabs.Panel>
                </Tabs>
            </Stack >

            {/* Add/Edit Modal */}
            < Modal
                opened={opened}
                onClose={close}
                title={editingAlarm ? 'Edit Alarm Adjustment' : 'Add Alarm Adjustment'}
                size="md"
            >
                <Stack gap="md">
                    {!editingAlarm && (
                        <>
                            <NumberInput
                                label="Alarm ID"
                                placeholder="Enter alarm ID"
                                value={formData.id}
                                onChange={(value) => setFormData({ ...formData, id: Number(value) || 0 })}
                                required
                            />
                            <NumberInput
                                label="Alarm Code"
                                placeholder="Enter alarm code"
                                value={formData.alarm_code}
                                onChange={(value) => setFormData({ ...formData, alarm_code: Number(value) || 0 })}
                                required
                            />
                            <NumberInput
                                label="Station Number"
                                placeholder="Enter station number"
                                value={formData.station_nr}
                                onChange={(value) => setFormData({ ...formData, station_nr: Number(value) || 0 })}
                                required
                            />
                        </>
                    )}
                    <TextInput
                        label="Time On"
                        placeholder="YYYY-MM-DD HH:MM:SS"
                        value={formData.time_on}
                        onChange={(e) => setFormData({ ...formData, time_on: e.target.value })}
                        description="Leave empty to keep unchanged"
                    />
                    <TextInput
                        label="Time Off"
                        placeholder="YYYY-MM-DD HH:MM:SS"
                        value={formData.time_off}
                        onChange={(e) => setFormData({ ...formData, time_off: e.target.value })}
                        description="Leave empty to keep unchanged"
                    />
                    <TextInput
                        label="Notes"
                        placeholder="Optional notes"
                        value={formData.notes}
                        onChange={(e) => setFormData({ ...formData, notes: e.target.value })}
                    />
                    <Group justify="flex-end" mt="md">
                        <Button variant="light" onClick={close}>
                            Cancel
                        </Button>
                        <Button
                            onClick={handleSubmit}
                            loading={addMutation.isPending || updateMutation.isPending}
                        >
                            {editingAlarm ? 'Update' : 'Add'}
                        </Button>
                    </Group>
                </Stack>
            </Modal >

            {/* Bulk Edit Modal */}
            < Modal
                opened={bulkEditOpened}
                onClose={closeBulkEdit}
                title={`Bulk Edit (${selectAllPages ? alarmsData?.total : selectedRecords.length} items)`
                }
                size="md"
            >
                <Stack gap="md">
                    <Text size="sm" c="dimmed">
                        Select fields to update. Only selected fields will be applied to all chosen adjustments.
                    </Text>

                    <Group align="flex-start">
                        <Checkbox
                            mt={8}
                            checked={bulkFormData.updateTimeOn}
                            onChange={(e) => setBulkFormData({ ...bulkFormData, updateTimeOn: e.currentTarget.checked })}
                        />
                        <TextInput
                            style={{ flex: 1 }}
                            label="Time On"
                            placeholder="YYYY-MM-DD HH:MM:SS"
                            value={bulkFormData.time_on}
                            onChange={(e) => setBulkFormData({ ...bulkFormData, time_on: e.target.value })}
                            disabled={!bulkFormData.updateTimeOn}
                            description="Leave empty to clear"
                        />
                    </Group>
                    <Group align="flex-start">
                        <Checkbox
                            mt={8}
                            checked={bulkFormData.updateTimeOff}
                            onChange={(e) => setBulkFormData({ ...bulkFormData, updateTimeOff: e.currentTarget.checked })}
                        />
                        <TextInput
                            style={{ flex: 1 }}
                            label="Time Off"
                            placeholder="YYYY-MM-DD HH:MM:SS"
                            value={bulkFormData.time_off}
                            onChange={(e) => setBulkFormData({ ...bulkFormData, time_off: e.target.value })}
                            disabled={!bulkFormData.updateTimeOff}
                            description="Leave empty to clear"
                        />
                    </Group>
                    <Group align="flex-start">
                        <Checkbox
                            mt={8}
                            checked={bulkFormData.updateNotes}
                            onChange={(e) => setBulkFormData({ ...bulkFormData, updateNotes: e.currentTarget.checked })}
                        />
                        <TextInput
                            style={{ flex: 1 }}
                            label="Notes"
                            placeholder="Notes..."
                            value={bulkFormData.notes}
                            onChange={(e) => setBulkFormData({ ...bulkFormData, notes: e.target.value })}
                            disabled={!bulkFormData.updateNotes}
                            description="Leave empty to clear"
                        />
                    </Group>

                    <Group justify="flex-end" mt="md">
                        <Button variant="light" onClick={closeBulkEdit}>
                            Cancel
                        </Button>
                        <Button
                            onClick={handleBulkSubmit}
                            loading={bulkUpdateMutation.isPending}
                        >
                            Update All
                        </Button>
                    </Group>
                </Stack>
            </Modal >
        </Container >
    )
}
