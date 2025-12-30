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

import { getAlarms, addAlarm, updateAlarm, deleteAlarm } from '../api'
import type { AlarmAdjustment } from '../api'

export default function Alarms() {
    const queryClient = useQueryClient()
    const [opened, { open, close }] = useDisclosure(false)
    const [editingAlarm, setEditingAlarm] = useState<AlarmAdjustment | null>(null)
    const [formData, setFormData] = useState({
        id: 0,
        alarm_code: 0,
        station_nr: 0,
        time_on: '',
        time_off: '',
        notes: '',
    })

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
                                        onChange={(e) => setFilters({ ...filters, alarm_code: e.target.value })}
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
                                        onChange={(e) => setFilters({ ...filters, station_nr: e.target.value })}
                                    />
                                ),
                                filtering: filters.station_nr !== '',
                            },
                            {
                                accessor: 'time_on',
                                title: 'Time On',
                                sortable: true,
                                render: (record) => record.time_on ? dayjs(record.time_on).format('YYYY-MM-DD HH:mm') : <Text size="sm" c="dimmed">—</Text>,
                            },
                            {
                                accessor: 'time_off',
                                title: 'Time Off',
                                sortable: true,
                                render: (record) => record.time_off ? dayjs(record.time_off).format('YYYY-MM-DD HH:mm') : <Text size="sm" c="dimmed">—</Text>,
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
                    />
                </Card>
            </Stack>

            {/* Add/Edit Modal */}
            <Modal
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
            </Modal>
        </Container>
    )
}
