import { useEffect, useRef } from 'react'
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
    ScrollArea
} from '@mantine/core'
import {
    IconAlertCircle,
    IconCheck,
    IconRefresh,
    IconExclamationCircle
} from '@tabler/icons-react'
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query'
import { getValidationReport, runValidation, getProcessingStatus } from '../api'

export default function DataIntegrity() {
    const queryClient = useQueryClient()
    const prevStatusRef = useRef<string | undefined>(undefined)

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

    // Auto-refresh report when processing completes
    useEffect(() => {
        if (prevStatusRef.current === 'running' && status?.status === 'completed') {
            queryClient.invalidateQueries({ queryKey: ['validationReport'] })
        }
        prevStatusRef.current = status?.status
    }, [status?.status, queryClient])

    const runValidationMutation = useMutation({
        mutationFn: runValidation,
        onSuccess: () => {
            // Invalidate status to kickstart polling (transition from idle -> running)
            queryClient.invalidateQueries({ queryKey: ['processingStatus'] })
            // Invalidate report (though it won't update until job finishes)
            queryClient.invalidateQueries({ queryKey: ['validationReport'] })
        }
    })

    const handleRunValidation = () => {
        runValidationMutation.mutate({})
    }

    if (isLoading) return <Loader />

    const hasRun = report?.last_run
    const hasIssues = (report?.summary?.total_issues ?? 0) > 0
    const isRunning = status?.status === 'running'

    // Check if the current/last process was validation related (simple heuristic or assumed global)
    // The user requested a specific "completed" badge style for the result. 
    // We'll show the status badge if running or recently completed.

    return (
        <Stack gap="lg" style={{ height: '100%' }}>
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

                    <Button
                        leftSection={<IconRefresh size={18} />}
                        loading={isRunning || runValidationMutation.isPending}
                        onClick={handleRunValidation}
                        disabled={isRunning}
                    >
                        Run Validation Scan
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
