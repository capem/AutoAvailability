import { useState } from 'react'
import {
    Container,
    Title,
    Text,
    Card,
    Group,
    Stack,
    Button,
    Select,
    Badge,
    Progress,
    SimpleGrid,
    Paper,
    ThemeIcon,
    Transition,
} from '@mantine/core'
import { DatePickerInput } from '@mantine/dates'
import { notifications } from '@mantine/notifications'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import {
    IconPlayerPlay,
    IconCalendar,
    IconDatabase,
    IconMail,
    IconFolder,
    IconCheck,
    IconX,
    IconPlayerStop,
    IconShieldCheck,
} from '@tabler/icons-react'
import dayjs from 'dayjs'

import { processData, abortProcessing, getProcessingStatus, getSystemStatus, runValidation, getAppSettings } from '../api'
import type { ProcessRequest } from '../api'

const updateModes = [
    { value: 'append', label: 'Append - Update while preserving deleted records' },
    { value: 'check', label: 'Check - Report changes without modifying' },
    { value: 'force-overwrite', label: 'Force Overwrite - Export fresh data' },
    { value: 'process-existing', label: 'Process Existing - Skip DB, use existing files' },
    { value: 'process-existing-except-alarms', label: 'Process Existing except Alarms - Update alarms only' },
]

export default function Dashboard() {
    const queryClient = useQueryClient()
    const [selectedDates, setSelectedDates] = useState<(Date | null)[]>([dayjs().subtract(1, 'day').toDate()])
    const [updateMode, setUpdateMode] = useState<string>('append')
    // Flag to track if we've synced with default settings
    const [settingsSynced, setSettingsSynced] = useState(false)

    const { data: processingStatus } = useQuery({
        queryKey: ['processingStatus'],
        queryFn: getProcessingStatus,
        refetchInterval: (query) => {
            const status = query.state.data?.status
            // Poll fast when active, stop when idle
            return status === 'running' || status === 'starting' ? 1000 : false
        },
    })

    const { data: systemStatus } = useQuery({
        queryKey: ['systemStatus'],
        queryFn: getSystemStatus,
        refetchInterval: 10000,
    })

    const { data: appSettings } = useQuery({
        queryKey: ['appSettings'],
        queryFn: getAppSettings,
        refetchOnWindowFocus: false,
    })

    // Load default update mode from settings when available
    if (appSettings && !settingsSynced) {
        setUpdateMode(appSettings.default_update_mode)
        setSettingsSynced(true)
    }

    const processMutation = useMutation({
        mutationFn: processData,
        onSuccess: () => {
            notifications.show({
                title: 'Processing Started',
                message: 'Data processing has been initiated',
                color: 'blue',
                icon: <IconPlayerPlay size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['processingStatus'] })
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

    const validationMutation = useMutation({
        mutationFn: runValidation,
        onSuccess: () => {
            notifications.show({
                title: 'Validation Started',
                message: 'Data validation scan has been initiated.',
                color: 'blue',
                icon: <IconShieldCheck size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['processingStatus'] })
        },
        onError: (error: Error) => {
            notifications.show({
                title: 'Error',
                message: error.message,
                color: 'red',
                icon: <IconX size={16} />,
            })
        }
    })

    const handleProcess = () => {
        if (selectedDates.length === 0) {
            notifications.show({
                title: 'No dates selected',
                message: 'Please select at least one date to process',
                color: 'yellow',
            })
            return
        }

        const request: ProcessRequest = {
            dates: selectedDates.filter((d): d is Date => d !== null).map((d) => dayjs(d).format('YYYY-MM-DD')),
            update_mode: updateMode as ProcessRequest['update_mode'],
        }

        processMutation.mutate(request)
    }

    const abortMutation = useMutation({
        mutationFn: abortProcessing,
        onSuccess: () => {
            notifications.show({
                title: 'Aborted',
                message: 'Processing was aborted',
                color: 'orange',
                icon: <IconPlayerStop size={16} />,
            })
            queryClient.invalidateQueries({ queryKey: ['processingStatus'] })
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

    const isProcessing = processingStatus?.status === 'running' || processingStatus?.status === 'starting'

    const getStatusIcon = (status: string) => {
        switch (status) {
            case 'ok':
                return <IconCheck size={14} />
            case 'warning':
                return <IconX size={14} />
            case 'error':
                return <IconX size={14} />
            default:
                return null
        }
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
                    <Title order={2} mb={4}>Dashboard</Title>
                    <Text c="dimmed">Process wind farm data and monitor system status</Text>
                </div>

                {/* Processing Card */}
                <Card shadow="sm" padding="lg" radius="md" withBorder>
                    <Card.Section withBorder inheritPadding py="xs" mb="md">
                        <Group justify="space-between">
                            <Text fw={500}>Data Processing</Text>
                            <Badge color={isProcessing ? 'blue' : 'gray'} variant="light">
                                {processingStatus?.status || 'idle'}
                            </Badge>
                        </Group>
                    </Card.Section>

                    <Stack gap="md">
                        <SimpleGrid cols={{ base: 1, sm: 2 }} spacing="md">
                            <DatePickerInput
                                type="multiple"
                                label="Select Date(s)"
                                placeholder="Pick dates to process"
                                value={selectedDates}
                                onChange={(value) => setSelectedDates(value as unknown as (Date | null)[])}
                                leftSection={<IconCalendar size={16} />}
                                maxDate={new Date()}
                                disabled={isProcessing}
                            />

                            <Select
                                label="Update Mode"
                                data={updateModes}
                                value={updateMode}
                                onChange={(value) => setUpdateMode(value || 'append')}
                                disabled={isProcessing}
                            />
                        </SimpleGrid>

                        {/* Processing Progress */}
                        <Transition mounted={isProcessing} transition="fade" duration={200}>
                            {(styles) => (
                                <Paper style={styles} p="md" radius="md" bg="dark.6">
                                    <Stack gap="xs">
                                        <Group justify="space-between">
                                            <Text size="sm" fw={500}>
                                                Processing: {processingStatus?.date || 'Initializing...'}
                                            </Text>
                                            <Text size="sm" c="dimmed">
                                                {processingStatus?.step}
                                            </Text>
                                        </Group>
                                        <Progress value={100} animated color="blue" size="sm" />
                                    </Stack>
                                </Paper>
                            )}
                        </Transition>

                        {/* Quick Actions */}
                        <Group>
                            <Button
                                leftSection={<IconPlayerPlay size={16} />}
                                onClick={handleProcess}
                                loading={processMutation.isPending}
                                disabled={isProcessing}
                                size="md"
                            >
                                Run Processing
                            </Button>
                            <Button
                                variant="outline"
                                leftSection={<IconShieldCheck size={16} />}
                                onClick={() => {
                                    const validDates = selectedDates.filter(d => d !== null) as Date[]
                                    let payload = {}

                                    if (validDates.length > 0) {
                                        const uniquePeriods = [...new Set(validDates.map(d => dayjs(d).format('YYYY-MM')))]
                                        const maxDate = validDates.reduce((max, d) => d > max ? d : max, validDates[0])
                                        payload = {
                                            dates: uniquePeriods,
                                            end_date: dayjs(maxDate).format('YYYY-MM-DD')
                                        }
                                    }
                                    validationMutation.mutate(payload)
                                }}
                                loading={validationMutation.isPending}
                                disabled={isProcessing}
                                size="md"
                            >
                                Run Validation
                            </Button>
                            {isProcessing && (
                                <Button
                                    leftSection={<IconPlayerStop size={16} />}
                                    onClick={() => abortMutation.mutate()}
                                    loading={abortMutation.isPending}
                                    color="red"
                                    variant="light"
                                    size="md"
                                >
                                    Abort
                                </Button>
                            )}
                            <Button
                                variant="light"
                                onClick={() => setSelectedDates([dayjs().subtract(1, 'day').toDate()])}
                                disabled={isProcessing}
                            >
                                Yesterday
                            </Button>
                            <Button
                                variant="light"
                                onClick={() => setSelectedDates([new Date()])}
                                disabled={isProcessing}
                            >
                                Today
                            </Button>
                        </Group>
                    </Stack>
                </Card>

                {/* System Status */}
                <Card shadow="sm" padding="lg" radius="md" withBorder>
                    <Card.Section withBorder inheritPadding py="xs" mb="md">
                        <Group justify="space-between">
                            <Text fw={500}>System Status</Text>
                            <Badge color={systemStatus?.overall === 'healthy' ? 'green' : 'yellow'} variant="light">
                                {systemStatus?.overall || 'checking...'}
                            </Badge>
                        </Group>
                    </Card.Section>

                    <SimpleGrid cols={{ base: 1, sm: 2, md: 4 }} spacing="md">
                        {systemStatus?.components?.map((component) => (
                            <Paper key={component.component} p="md" radius="md" withBorder>
                                <Group gap="sm" mb="xs">
                                    <ThemeIcon
                                        size="sm"
                                        radius="xl"
                                        color={getStatusColor(component.status)}
                                        variant="light"
                                    >
                                        {component.component.includes('Database') && <IconDatabase size={12} />}
                                        {component.component.includes('Email') && <IconMail size={12} />}
                                        {component.component.includes('Directory') && <IconFolder size={12} />}
                                        {!component.component.includes('Database') &&
                                            !component.component.includes('Email') &&
                                            !component.component.includes('Directory') &&
                                            getStatusIcon(component.status)}
                                    </ThemeIcon>
                                    <Text size="sm" fw={500}>{component.component}</Text>
                                </Group>
                                <Text size="xs" c="dimmed" lineClamp={2}>
                                    {component.details}
                                </Text>
                            </Paper>
                        ))}
                    </SimpleGrid>
                </Card>
            </Stack>
        </Container>
    )
}
