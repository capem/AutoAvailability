import { useState, useEffect } from 'react'
import {
    Container,
    Title,
    Text,
    Card,
    Stack,
    SimpleGrid,
    Paper,
    Group,
    ThemeIcon,
    Badge,
    LoadingOverlay,
    Switch,
    Select,
    NumberInput,
    Button,
    Alert,
    Divider,
} from '@mantine/core'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { notifications } from '@mantine/notifications'
import {
    IconDatabase,
    IconMail,
    IconFolder,
    IconCheck,
    IconClock,
    IconPlayerPlay,
    IconAlertCircle,
    IconCalendarEvent,
} from '@tabler/icons-react'

import { getSystemStatus, getSchedulerStatus, configureScheduler, triggerScheduler, type SchedulerStatus } from '../api'

const DAYS_OF_WEEK = [
    { value: 'mon', label: 'Monday' },
    { value: 'tue', label: 'Tuesday' },
    { value: 'wed', label: 'Wednesday' },
    { value: 'thu', label: 'Thursday' },
    { value: 'fri', label: 'Friday' },
    { value: 'sat', label: 'Saturday' },
    { value: 'sun', label: 'Sunday' },
]

export default function Settings() {
    const queryClient = useQueryClient()

    const { data: systemStatus, isLoading } = useQuery({
        queryKey: ['systemStatus'],
        queryFn: getSystemStatus,
        refetchInterval: 10000,
    })

    const { data: schedulerStatus, isLoading: schedulerLoading } = useQuery({
        queryKey: ['schedulerStatus'],
        queryFn: getSchedulerStatus,
        refetchInterval: 5000,
    })

    // Local state for form
    const [enabled, setEnabled] = useState(false)
    const [dayOfWeek, setDayOfWeek] = useState('mon')
    const [hour, setHour] = useState<number>(6)
    const [minute, setMinute] = useState<number>(0)
    const [hasChanges, setHasChanges] = useState(false)

    // Sync form state with server state
    useEffect(() => {
        if (schedulerStatus) {
            setEnabled(schedulerStatus.enabled)
            setDayOfWeek(schedulerStatus.day_of_week)
            setHour(schedulerStatus.hour)
            setMinute(schedulerStatus.minute)
            setHasChanges(false)
        }
    }, [schedulerStatus])

    const configureMutation = useMutation({
        mutationFn: configureScheduler,
        onSuccess: () => {
            queryClient.invalidateQueries({ queryKey: ['schedulerStatus'] })
            notifications.show({
                title: 'Scheduler Updated',
                message: 'Schedule configuration saved successfully',
                color: 'green',
            })
            setHasChanges(false)
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
            })
        },
    })

    const triggerMutation = useMutation({
        mutationFn: triggerScheduler,
        onSuccess: (data) => {
            queryClient.invalidateQueries({ queryKey: ['schedulerStatus'] })
            notifications.show({
                title: 'Job Triggered',
                message: data.message || 'Scheduled job has been triggered',
                color: 'blue',
            })
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Trigger Failed',
                message: error.message,
                color: 'red',
            })
        },
    })

    const handleSave = () => {
        configureMutation.mutate({
            enabled,
            day_of_week: dayOfWeek,
            hour,
            minute,
        })
    }

    const handleFieldChange = (setter: (val: any) => void, val: any) => {
        setter(val)
        setHasChanges(true)
    }

    const formatNextRun = (nextRun: string | null) => {
        if (!nextRun) return 'Not scheduled'
        const date = new Date(nextRun)
        return date.toLocaleString()
    }

    const formatLastRun = (lastRun: string | null) => {
        if (!lastRun) return 'Never'
        const date = new Date(lastRun)
        return date.toLocaleString()
    }

    const getIcon = (component: string) => {
        if (component.includes('Database')) return IconDatabase
        if (component.includes('Email')) return IconMail
        if (component.includes('Directory')) return IconFolder
        return IconCheck
    }

    const getStatusColor = (status: string) => {
        switch (status) {
            case 'ok':
                return 'green'
            case 'warning':
                return 'yellow'
            case 'error':
                return 'red'
            default:
                return 'gray'
        }
    }

    return (
        <Container size="lg">
            <Stack gap="xl">
                {/* Page Title */}
                <div>
                    <Title order={2} mb={4}>Settings & Configuration</Title>
                    <Text c="dimmed">View system configuration and manage scheduled processing</Text>
                </div>

                {/* Scheduled Processing */}
                <Card shadow="sm" padding="lg" radius="md" withBorder pos="relative">
                    <LoadingOverlay visible={schedulerLoading} />

                    <Card.Section withBorder inheritPadding py="xs" mb="md">
                        <Group justify="space-between">
                            <Group gap="sm">
                                <ThemeIcon size="md" radius="md" color="blue" variant="light">
                                    <IconCalendarEvent size={16} />
                                </ThemeIcon>
                                <Text fw={500}>Automated Weekly Processing</Text>
                            </Group>
                            <Badge
                                color={schedulerStatus?.enabled ? 'green' : 'gray'}
                                size="lg"
                                variant="light"
                            >
                                {schedulerStatus?.enabled ? 'Active' : 'Disabled'}
                            </Badge>
                        </Group>
                    </Card.Section>

                    <Stack gap="md">
                        {/* Last Run Status */}
                        {schedulerStatus?.last_run && (
                            <Alert
                                color={schedulerStatus.last_status === 'error' ? 'red' : 'green'}
                                icon={schedulerStatus.last_status === 'error' ? <IconAlertCircle /> : <IconCheck />}
                                title={`Last Run: ${schedulerStatus.last_status === 'error' ? 'Failed' : 'Success'}`}
                            >
                                <Text size="sm">
                                    {formatLastRun(schedulerStatus.last_run)}
                                    {schedulerStatus.last_error && (
                                        <>
                                            <br />
                                            <Text c="red" size="xs" mt="xs">
                                                Error: {schedulerStatus.last_error}
                                            </Text>
                                        </>
                                    )}
                                </Text>
                            </Alert>
                        )}

                        {/* Enable Toggle */}
                        <Switch
                            label="Enable weekly automated processing"
                            description="Automatically process yesterday's data on a weekly schedule"
                            checked={enabled}
                            onChange={(e) => handleFieldChange(setEnabled, e.currentTarget.checked)}
                            size="md"
                        />

                        <Divider />

                        {/* Schedule Configuration */}
                        <SimpleGrid cols={{ base: 1, sm: 3 }} spacing="md">
                            <Select
                                label="Day of Week"
                                data={DAYS_OF_WEEK}
                                value={dayOfWeek}
                                onChange={(val) => handleFieldChange(setDayOfWeek, val || 'mon')}
                                disabled={!enabled}
                                leftSection={<IconCalendarEvent size={16} />}
                            />
                            <NumberInput
                                label="Hour (0-23)"
                                min={0}
                                max={23}
                                value={hour}
                                onChange={(val) => handleFieldChange(setHour, val || 0)}
                                disabled={!enabled}
                                leftSection={<IconClock size={16} />}
                            />
                            <NumberInput
                                label="Minute (0-59)"
                                min={0}
                                max={59}
                                value={minute}
                                onChange={(val) => handleFieldChange(setMinute, val || 0)}
                                disabled={!enabled}
                            />
                        </SimpleGrid>

                        {/* Next Run Info */}
                        {schedulerStatus?.enabled && schedulerStatus?.next_run && (
                            <Paper p="sm" radius="md" withBorder bg="blue.0">
                                <Group gap="xs">
                                    <IconClock size={16} />
                                    <Text size="sm">
                                        <strong>Next scheduled run:</strong> {formatNextRun(schedulerStatus.next_run)}
                                    </Text>
                                </Group>
                            </Paper>
                        )}

                        <Divider />

                        {/* Actions */}
                        <Group>
                            <Button
                                onClick={handleSave}
                                loading={configureMutation.isPending}
                                disabled={!hasChanges}
                            >
                                Save Configuration
                            </Button>
                            <Button
                                variant="light"
                                leftSection={<IconPlayerPlay size={16} />}
                                onClick={() => triggerMutation.mutate()}
                                loading={triggerMutation.isPending}
                            >
                                Run Now
                            </Button>
                        </Group>
                    </Stack>
                </Card>

                {/* System Health */}
                <Card shadow="sm" padding="lg" radius="md" withBorder pos="relative">
                    <LoadingOverlay visible={isLoading} />

                    <Card.Section withBorder inheritPadding py="xs" mb="md">
                        <Group justify="space-between">
                            <Text fw={500}>System Health</Text>
                            <Badge
                                color={systemStatus?.overall === 'healthy' ? 'green' : 'yellow'}
                                size="lg"
                            >
                                {systemStatus?.overall || 'checking...'}
                            </Badge>
                        </Group>
                    </Card.Section>

                    <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="md">
                        {systemStatus?.components?.map((component) => {
                            const Icon = getIcon(component.component)
                            return (
                                <Paper key={component.component} p="md" radius="md" withBorder>
                                    <Group justify="space-between" mb="xs">
                                        <Group gap="sm">
                                            <ThemeIcon
                                                size="md"
                                                radius="md"
                                                color={getStatusColor(component.status)}
                                                variant="light"
                                            >
                                                <Icon size={16} />
                                            </ThemeIcon>
                                            <Text fw={500}>{component.component}</Text>
                                        </Group>
                                        <Badge color={getStatusColor(component.status)} variant="dot">
                                            {component.status}
                                        </Badge>
                                    </Group>
                                    <Text size="sm" c="dimmed" ml={42}>
                                        {component.details}
                                    </Text>
                                </Paper>
                            )
                        })}
                    </SimpleGrid>
                </Card>

                {/* Info Card */}
                <Card shadow="sm" padding="lg" radius="md" withBorder>
                    <Card.Section withBorder inheritPadding py="xs" mb="md">
                        <Text fw={500}>About</Text>
                    </Card.Section>

                    <Stack gap="xs">
                        <Group justify="space-between">
                            <Text c="dimmed">Application</Text>
                            <Text>Wind Farm Data Processing System</Text>
                        </Group>
                        <Group justify="space-between">
                            <Text c="dimmed">Version</Text>
                            <Badge variant="light">1.0.0</Badge>
                        </Group>
                        <Group justify="space-between">
                            <Text c="dimmed">Backend API</Text>
                            <Text size="sm">http://localhost:8000</Text>
                        </Group>
                    </Stack>
                </Card>
            </Stack>
        </Container>
    )
}
