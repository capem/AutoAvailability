import { useState } from 'react'
import {
    Container,
    Title,
    Text,
    Card,
    Table,
    Button,
    Group,
    Stack,
    Modal,
    TextInput,
    NumberInput,
    ActionIcon,
    Badge,
    Tooltip,
    LoadingOverlay,
} from '@mantine/core'
import { useDisclosure } from '@mantine/hooks'
import { notifications } from '@mantine/notifications'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { IconPlus, IconEdit, IconTrash, IconCheck, IconX } from '@tabler/icons-react'
import dayjs from 'dayjs'

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

    const { data: alarmsData, isLoading } = useQuery({
        queryKey: ['alarms'],
        queryFn: getAlarms,
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
                <Card shadow="sm" padding="lg" radius="md" withBorder pos="relative">
                    <LoadingOverlay visible={isLoading} />

                    {adjustments.length === 0 ? (
                        <Text c="dimmed" ta="center" py="xl">
                            No manual adjustments found
                        </Text>
                    ) : (
                        <Table striped highlightOnHover>
                            <Table.Thead>
                                <Table.Tr>
                                    <Table.Th>ID</Table.Th>
                                    <Table.Th>Alarm Code</Table.Th>
                                    <Table.Th>Station Nr</Table.Th>
                                    <Table.Th>Time On</Table.Th>
                                    <Table.Th>Time Off</Table.Th>
                                    <Table.Th>Notes</Table.Th>
                                    <Table.Th>Last Updated</Table.Th>
                                    <Table.Th>Actions</Table.Th>
                                </Table.Tr>
                            </Table.Thead>
                            <Table.Tbody>
                                {adjustments.map((alarm, index) => (
                                    <Table.Tr key={`${alarm.id}-${index}`}>
                                        <Table.Td>
                                            <Badge variant="light">{alarm.id}</Badge>
                                        </Table.Td>
                                        <Table.Td>{alarm.alarm_code}</Table.Td>
                                        <Table.Td>{alarm.station_nr}</Table.Td>
                                        <Table.Td>
                                            {alarm.time_on ? (
                                                <Text size="sm">{dayjs(alarm.time_on).format('YYYY-MM-DD HH:mm')}</Text>
                                            ) : (
                                                <Text size="sm" c="dimmed">—</Text>
                                            )}
                                        </Table.Td>
                                        <Table.Td>
                                            {alarm.time_off ? (
                                                <Text size="sm">{dayjs(alarm.time_off).format('YYYY-MM-DD HH:mm')}</Text>
                                            ) : (
                                                <Text size="sm" c="dimmed">—</Text>
                                            )}
                                        </Table.Td>
                                        <Table.Td>
                                            <Text size="sm" lineClamp={1} maw={150}>
                                                {alarm.notes || '—'}
                                            </Text>
                                        </Table.Td>
                                        <Table.Td>
                                            <Text size="xs" c="dimmed">
                                                {alarm.last_updated
                                                    ? dayjs(alarm.last_updated).format('YYYY-MM-DD HH:mm')
                                                    : '—'}
                                            </Text>
                                        </Table.Td>
                                        <Table.Td>
                                            <Group gap={4}>
                                                <Tooltip label="Edit">
                                                    <ActionIcon
                                                        variant="subtle"
                                                        color="blue"
                                                        onClick={() => handleOpenEdit(alarm)}
                                                    >
                                                        <IconEdit size={16} />
                                                    </ActionIcon>
                                                </Tooltip>
                                                <Tooltip label="Delete">
                                                    <ActionIcon
                                                        variant="subtle"
                                                        color="red"
                                                        onClick={() => handleDelete(alarm.id)}
                                                        loading={deleteMutation.isPending}
                                                    >
                                                        <IconTrash size={16} />
                                                    </ActionIcon>
                                                </Tooltip>
                                            </Group>
                                        </Table.Td>
                                    </Table.Tr>
                                ))}
                            </Table.Tbody>
                        </Table>
                    )}
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
