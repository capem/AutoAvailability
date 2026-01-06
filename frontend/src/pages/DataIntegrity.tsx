import { useEffect, useRef, useState } from 'react'
import {
    Title,
    Text,
    Paper,
    Stack,
    Group,
    Button,
    Alert,
    Card,
    Grid,
    Badge,
    Accordion,
    Table,
    Loader,
    ScrollArea,
    Drawer,
    NumberInput,
    Checkbox,
    SimpleGrid,
    Code,
    ActionIcon,
    Tooltip,
    Tabs
} from '@mantine/core'
import { DatePickerInput } from '@mantine/dates'
import {
    IconAlertCircle,
    IconCheck,
    IconRefresh,
    IconExclamationCircle,
    IconSettings,
    IconCalendar
} from '@tabler/icons-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getValidationReport, runValidation, getProcessingStatus, getValidationRules } from '../api'
import type { ValidationRequest } from '../api'

export default function DataIntegrity() {
    const queryClient = useQueryClient()
    const prevStatusRef = useRef<string | undefined>(undefined)

    // UI State
    const [settingsOpen, setSettingsOpen] = useState(false)
    const [dateRange, setDateRange] = useState<[Date | null, Date | null]>([null, null])
    const [stuckIntervals, setStuckIntervals] = useState<number | undefined>(undefined)
    const [excludeZero, setExcludeZero] = useState(false)

    const { data: report, isLoading } = useQuery({
        queryKey: ['validationReport'],
        queryFn: getValidationReport,
    })

    const { data: status } = useQuery({
        queryKey: ['processingStatus'],
        queryFn: getProcessingStatus,
        refetchInterval: (query) => {
            const currentStatus = query.state.data?.status
            // Only poll if a process is active
            return currentStatus === 'running' || currentStatus === 'starting' ? 1000 : false
        },
    })

    const { data: rules } = useQuery({
        queryKey: ['validationRules'],
        queryFn: getValidationRules,
    })

    // Initialize defaults from rules when loaded
    useEffect(() => {
        if (rules && stuckIntervals === undefined) {
            setStuckIntervals(rules.defaults.stuck_intervals)
        }
    }, [rules, stuckIntervals])

    // Auto-refresh report when processing completes
    useEffect(() => {
        if (prevStatusRef.current === 'running' && status?.status === 'completed') {
            queryClient.invalidateQueries({ queryKey: ['validationReport'] })
        }
        prevStatusRef.current = status?.status
    }, [status?.status, queryClient])

    const runValidationMutation = useMutation({
        mutationFn: (req: ValidationRequest) => runValidation(req),
        onSuccess: () => {
            // Invalidate status to kickstart polling (transition from idle -> running)
            queryClient.invalidateQueries({ queryKey: ['processingStatus'] })
            // Invalidate report (though it won't update until job finishes)
            queryClient.invalidateQueries({ queryKey: ['validationReport'] })
            setSettingsOpen(false)
        }
    })

    const handleRunValidation = () => {
        const request: ValidationRequest = {
            stuck_intervals: stuckIntervals,
            exclude_zero: excludeZero,
        }

        if (dateRange[0]) {
            // Adjust to local date string YYYY-MM-DD
            const offset = dateRange[0].getTimezoneOffset()
            const start = new Date(dateRange[0].getTime() - (offset * 60 * 1000))
            request.start_date = start.toISOString().split('T')[0]
        }

        if (dateRange[1]) {
            const offset = dateRange[1].getTimezoneOffset()
            const end = new Date(dateRange[1].getTime() - (offset * 60 * 1000))
            request.end_date = end.toISOString().split('T')[0]
        }

        runValidationMutation.mutate(request)
    }

    if (isLoading) return <Loader />

    const hasRun = report?.last_run
    const hasIssues = (report?.summary?.total_issues ?? 0) > 0
    const isRunning = status?.status === 'running'

    return (
        <Stack gap="lg" style={{ height: '100%' }}>
            <Drawer
                opened={settingsOpen}
                onClose={() => setSettingsOpen(false)}
                title="Validation Settings"
                position="right"
                size="lg" // Increased size for better layout
                padding="md"
            >
                <Tabs defaultValue="config">
                    <Tabs.List grow>
                        <Tabs.Tab value="config" leftSection={<IconSettings size={16} />}>
                            Configuration
                        </Tabs.Tab>
                        <Tabs.Tab value="reference" leftSection={<IconExclamationCircle size={16} />}>
                            Rules & Info
                        </Tabs.Tab>
                    </Tabs.List>

                    <Tabs.Panel value="config">
                        <Stack gap="xl" pt="xl">
                            <Card withBorder padding="md" radius="md">
                                <Stack gap="md">
                                    <Group justify="space-between">
                                        <Group gap="xs">
                                            <IconCalendar size={18} color="gray" />
                                            <Text fw={600}>Scan Scope</Text>
                                        </Group>
                                    </Group>
                                    <Text size="sm" c="dimmed">
                                        Define the time period to analyze. Leave empty to scan all available data.
                                    </Text>
                                    <DatePickerInput
                                        type="range"
                                        placeholder="Select date range"
                                        value={dateRange}
                                        onChange={(value) => setDateRange(value as [Date | null, Date | null])}
                                        clearable
                                    />
                                </Stack>
                            </Card>

                            <Card withBorder padding="md" radius="md">
                                <Stack gap="md">
                                    <Group gap="xs">
                                        <IconAlertCircle size={18} color="gray" />
                                        <Text fw={600}>Sensitivity Parameters</Text>
                                    </Group>

                                    <Stack gap="xs">
                                        <Text size="sm" fw={500}>Stuck Value Detection</Text>
                                        <Text size="xs" c="dimmed" mb="xs">
                                            Adjust how aggressive the stuck value detection should be.
                                        </Text>

                                        <SimpleGrid cols={2}>
                                            <NumberInput
                                                label="Stuck Interval Threshold"
                                                description="Consecutive identical readings required to flag."
                                                value={stuckIntervals}
                                                onChange={(val) => setStuckIntervals(Number(val))}
                                                min={2}
                                                max={100}
                                            />
                                            <Stack justify="center" pt="xs">
                                                <Checkbox
                                                    label="Exclude Zero Values"
                                                    description="Ignore 0.0 sequences (e.g. calm wind)"
                                                    checked={excludeZero}
                                                    onChange={(event) => setExcludeZero(event.currentTarget.checked)}
                                                />
                                            </Stack>
                                        </SimpleGrid>
                                    </Stack>
                                </Stack>
                            </Card>

                            <Button
                                size="md"
                                onClick={handleRunValidation}
                                loading={isRunning || runValidationMutation.isPending}
                                disabled={isRunning}
                            >
                                Run Validation Scan
                            </Button>
                        </Stack>
                    </Tabs.Panel>

                    <Tabs.Panel value="reference">
                        <Stack gap="xl" pt="xl">
                            <Stack gap="xs">
                                <Text fw={600} size="sm" tt="uppercase" c="dimmed">Immutable Rules</Text>
                                <Card withBorder>
                                    <Text size="sm" mb="md">
                                        The following physical limits are hardcoded for quality control. Values outside these ranges are flagged as <b>Out of Range</b>.
                                    </Text>
                                    {rules && (
                                        <Table highlightOnHover>
                                            <Table.Thead>
                                                <Table.Tr>
                                                    <Table.Th>Sensor</Table.Th>
                                                    <Table.Th>Valid Range</Table.Th>
                                                </Table.Tr>
                                            </Table.Thead>
                                            <Table.Tbody>
                                                {Object.entries(rules.ranges).map(([sensor, range]) => (
                                                    <Table.Tr key={sensor}>
                                                        <Table.Td fw={500}>{sensor}</Table.Td>
                                                        <Table.Td><Code>{`[${range[0]}, ${range[1]}]`}</Code></Table.Td>
                                                    </Table.Tr>
                                                ))}
                                            </Table.Tbody>
                                        </Table>
                                    )}
                                </Card>
                            </Stack>

                            <Stack gap="xs">
                                <Text fw={600} size="sm" tt="uppercase" c="dimmed">Methodology</Text>
                                <Accordion variant="separated" defaultValue="completeness">
                                    <Accordion.Item value="completeness">
                                        <Accordion.Control icon={<IconCheck size={18} color="teal" />}>Completeness Checks</Accordion.Control>
                                        <Accordion.Panel>
                                            <Stack gap="sm">
                                                <Group align="flex-start" wrap="nowrap">
                                                    <Badge color="red" size="sm" variant="dot">System Outages</Badge>
                                                    <Text size="sm">Times where NO stations reported data.</Text>
                                                </Group>
                                                <Group align="flex-start" wrap="nowrap">
                                                    <Badge color="orange" size="sm" variant="dot">Station Gaps</Badge>
                                                    <Text size="sm">Times missing for a specific station.</Text>
                                                </Group>
                                                <Group align="flex-start" wrap="nowrap">
                                                    <Badge color="cyan" size="sm" variant="dot">Sensor Gaps</Badge>
                                                    <Text size="sm">Specific sensors missing data (NaN) while station is connected.</Text>
                                                </Group>
                                                <Group align="flex-start" wrap="nowrap">
                                                    <Badge color="yellow" size="sm" variant="dot">Empty Rows</Badge>
                                                    <Text size="sm">Rows present but all sensor values are NaN.</Text>
                                                </Group>
                                            </Stack>
                                        </Accordion.Panel>
                                    </Accordion.Item>

                                    <Accordion.Item value="integrity">
                                        <Accordion.Control icon={<IconAlertCircle size={18} color="orange" />}>Integrity Checks</Accordion.Control>
                                        <Accordion.Panel>
                                            <Stack gap="sm">
                                                <Group align="flex-start" wrap="nowrap">
                                                    <Badge color="blue" size="sm" variant="dot">Stuck Values</Badge>
                                                    <Text size="sm">Sensors detecting the exact same value (mean, min, max, stddev) for <i>{stuckIntervals ?? 3}</i> consecutive intervals.</Text>
                                                </Group>
                                                <Group align="flex-start" wrap="nowrap">
                                                    <Badge color="cyan" size="sm" variant="outline">Range Checks</Badge>
                                                    <Text size="sm">Values outside the defined physical valid ranges.</Text>
                                                </Group>
                                            </Stack>
                                        </Accordion.Panel>
                                    </Accordion.Item>
                                </Accordion>
                            </Stack>
                        </Stack>
                    </Tabs.Panel>
                </Tabs>
            </Drawer>

            <Group justify="space-between">
                <div>
                    <Title order={2}>Data Integrity Report</Title>
                    <Text c="dimmed">
                        {hasRun
                            ? `Last run: ${new Date(report.last_run!).toLocaleString()}`
                            : 'No validation report found'}
                    </Text>
                </div>
                <Group>
                    {isRunning && (
                        <Badge variant="dot" size="lg" color="blue">
                            Running: {status.step || 'Processing...'}
                        </Badge>
                    )}
                    {status?.status === 'completed' && status.step === null && (
                        <Badge variant="dot" size="lg" color="green">
                            Completed
                        </Badge>
                    )}

                    <Tooltip label="Configure Scan Settings">
                        <ActionIcon
                            size="lg"
                            variant="light"
                            onClick={() => setSettingsOpen(true)}
                            disabled={isRunning}
                        >
                            <IconSettings size={20} />
                        </ActionIcon>
                    </Tooltip>

                    <Button
                        leftSection={<IconRefresh size={18} />}
                        loading={isRunning || runValidationMutation.isPending}
                        onClick={handleRunValidation}
                        disabled={isRunning}
                    >
                        Quick Run (Defaults)
                    </Button>
                </Group>
            </Group>

            {!hasRun && (
                <Alert icon={<IconAlertCircle size={16} />} title="No Report" color="blue">
                    No validation report has been generated yet. Click "Run Validation Scan" to check data integrity.
                </Alert>
            )}

            {hasRun && (
                <>
                    {/* Summary Cards */}
                    <Grid>
                        <Grid.Col span={{ base: 6, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Files Scanned</Text>
                                    <Text size="xl" fw={700}>{report.summary.total_files_scanned}</Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 6, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Total Issues</Text>
                                    <Group gap="xs">
                                        <Text size="xl" fw={700} c={hasIssues ? 'red' : 'green'}>
                                            {report.summary.total_issues}
                                        </Text>
                                        {!hasIssues && <IconCheck color="green" />}
                                    </Group>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 12, sm: 6, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">System Outages</Text>
                                    <Text size="xl" fw={700} c={(report.summary.system_issues_count ?? 0) > 0 ? "red" : "dimmed"}>
                                        {report.summary.system_issues_count || 0}
                                    </Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 12, sm: 4, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Station Gaps</Text>
                                    <Text size="xl" fw={700} c={(report.summary.completeness_issues_count ?? 0) > 0 ? "orange" : "dimmed"}>
                                        {report.summary.completeness_issues_count ?? 0}
                                    </Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 12, sm: 4, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Empty Rows</Text>
                                    <Text size="xl" fw={700} c={(report.summary.empty_rows_count ?? 0) > 0 ? "yellow" : "dimmed"}>
                                        {report.summary.empty_rows_count ?? 0}
                                    </Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 12, sm: 4, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Sensor Gaps</Text>
                                    <Text size="xl" fw={700} c={(report.summary.sensor_gaps_count ?? 0) > 0 ? "cyan" : "dimmed"}>
                                        {report.summary.sensor_gaps_count ?? 0}
                                    </Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 12, sm: 6, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Stuck Values</Text>
                                    <Text size="xl" fw={700} c={report.summary.stuck_values_count > 0 ? "orange" : "dimmed"}>
                                        {report.summary.stuck_values_count}
                                    </Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                        <Grid.Col span={{ base: 12, sm: 6, md: 2 }}>
                            <Card withBorder padding="lg">
                                <Stack gap="xs">
                                    <Text size="xs" tt="uppercase" fw={700} c="dimmed">Out of Range</Text>
                                    <Text size="xl" fw={700} c={report.summary.out_of_range_count > 0 ? "orange" : "dimmed"}>
                                        {report.summary.out_of_range_count}
                                    </Text>
                                </Stack>
                            </Card>
                        </Grid.Col>
                    </Grid>

                    {/* Detailed Issues */}
                    {hasIssues ? (
                        <Paper withBorder p="md" style={{ flex: 1, display: 'flex', flexDirection: 'column', minHeight: 0 }}>
                            <Title order={4} mb="md">Detailed Findings</Title>
                            <ScrollArea>
                                <Accordion variant="separated">
                                    {report.details.map((fileReport: any) => (
                                        <Accordion.Item key={fileReport.file} value={fileReport.file}>
                                            <Accordion.Control icon={<IconExclamationCircle size={16} color="red" />}>
                                                <Group justify="space-between" mr="xl">
                                                    <Text fw={500}>{fileReport.file}</Text>
                                                    <Badge color="red">{fileReport.issues.length} Issues</Badge>
                                                </Group>
                                            </Accordion.Control>
                                            <Accordion.Panel>
                                                <Table striped highlightOnHover>
                                                    <Table.Thead>
                                                        <Table.Tr>
                                                            <Table.Th>Type</Table.Th>
                                                            <Table.Th>Station</Table.Th>
                                                            <Table.Th>Details</Table.Th>
                                                            <Table.Th>Count</Table.Th>
                                                            <Table.Th>Range</Table.Th>
                                                            <Table.Th>Value/Bounds</Table.Th>
                                                        </Table.Tr>
                                                    </Table.Thead>
                                                    <Table.Tbody>
                                                        {[...fileReport.issues].sort((a: any, b: any) => {
                                                            // Helper for sort precedence
                                                            const getSeverity = (issue: any) => {
                                                                if (issue.type === 'system_completeness' && issue.sensor === 'Global Connectivity') return 0 // Site Blackout
                                                                if (issue.type === 'system_completeness') return 1 // Sensor Outage
                                                                if (issue.type === 'completeness') return 2 // Station Gap
                                                                if (issue.type === 'empty_row') return 2.5 // Empty Row
                                                                if (issue.type === 'sensor_gap') return 2.8 // Sensor Gap
                                                                if (issue.type === 'stuck_value') return 3 // Stuck
                                                                if (issue.type === 'out_of_range') return 4 // Range
                                                                return 99
                                                            }
                                                            return getSeverity(a) - getSeverity(b)
                                                        }).map((issue: any, idx: number) => {
                                                            const isBlackout = issue.type === 'system_completeness' && issue.sensor === 'Global Connectivity'
                                                            const isSensorOutage = issue.type === 'system_completeness' && !isBlackout
                                                            const isStationGap = issue.type === 'completeness'
                                                            const isEmptyRow = issue.type === 'empty_row'
                                                            const isSensorGap = issue.type === 'sensor_gap'
                                                            const isStuck = issue.type === 'stuck_value'

                                                            let badgeLabel = 'Unknown'
                                                            let badgeColor = 'gray'
                                                            let badgeVariant = 'light'

                                                            if (isBlackout) {
                                                                badgeLabel = 'SITE BLACKOUT'
                                                                badgeColor = 'red'
                                                                badgeVariant = 'filled'
                                                            } else if (isSensorOutage) {
                                                                badgeLabel = 'SENSOR OUTAGE'
                                                                badgeColor = 'orange'
                                                                badgeVariant = 'filled'
                                                            } else if (isStationGap) {
                                                                badgeLabel = 'STATION GAP'
                                                                badgeColor = 'orange'
                                                                badgeVariant = 'light'
                                                            } else if (isEmptyRow) {
                                                                badgeLabel = 'EMPTY DATA'
                                                                badgeColor = 'yellow'
                                                                badgeVariant = 'light'
                                                            } else if (isSensorGap) {
                                                                badgeLabel = 'SENSOR GAP'
                                                                badgeColor = 'cyan'
                                                                badgeVariant = 'light'
                                                            } else if (isStuck) {
                                                                badgeLabel = 'STUCK VALUE'
                                                                badgeColor = 'blue'
                                                                badgeVariant = 'light'
                                                            } else {
                                                                badgeLabel = 'OUT OF RANGE'
                                                                badgeColor = 'cyan'
                                                                badgeVariant = 'light'
                                                            }

                                                            return (
                                                                <Table.Tr key={idx}>
                                                                    <Table.Td>
                                                                        <Badge
                                                                            color={badgeColor}
                                                                            variant={badgeVariant}
                                                                        >
                                                                            {badgeLabel}
                                                                        </Badge>
                                                                    </Table.Td>
                                                                    <Table.Td>{issue.station_id}</Table.Td>
                                                                    <Table.Td>
                                                                        {issue.sensor || issue.column}
                                                                        {(issue.type === 'completeness' || issue.type === 'system_completeness') && (
                                                                            <Text size="xs" c="dimmed">
                                                                                {issue.completeness_pct}% Complete
                                                                                (Exp: {issue.total_expected})
                                                                            </Text>
                                                                        )}
                                                                    </Table.Td>
                                                                    <Table.Td>{issue.count}</Table.Td>
                                                                    <Table.Td>
                                                                        <Text size="xs">
                                                                            {new Date(issue.range_start).toLocaleString()} - <br />
                                                                            {new Date(issue.range_end).toLocaleString()}
                                                                        </Text>
                                                                    </Table.Td>
                                                                    <Table.Td>
                                                                        {issue.sample_value ? `Sample: ${issue.sample_value}` : ''}
                                                                        {issue.bounds ? `Bounds: ${issue.bounds.join('-')}` : ''}
                                                                        {issue.missing_timestamps && (
                                                                            <Accordion variant="subtle">
                                                                                <Accordion.Item value="timestamps">
                                                                                    <Accordion.Control p={0} h={24}>Show Missing</Accordion.Control>
                                                                                    <Accordion.Panel>
                                                                                        <Text size="xs" style={{ whiteSpace: 'pre-wrap' }}>
                                                                                            {issue.missing_timestamps.join('\n')}
                                                                                        </Text>
                                                                                    </Accordion.Panel>
                                                                                </Accordion.Item>
                                                                            </Accordion>
                                                                        )}
                                                                    </Table.Td>
                                                                </Table.Tr>
                                                            )
                                                        })}
                                                    </Table.Tbody>
                                                </Table>
                                            </Accordion.Panel>
                                        </Accordion.Item>
                                    ))}
                                </Accordion>
                            </ScrollArea>
                        </Paper>
                    ) : (
                        <Alert icon={<IconCheck size={16} />} title="Clean Data" color="green">
                            No integrity issues were found in the scanned files.
                        </Alert>
                    )}
                </>
            )}
        </Stack>
    )
}
