import { useState } from 'react'
import {
    Container,
    Title,
    Text,
    Card,
    Stack,
    Code,
    ScrollArea,
    Group,
    Button,
    NumberInput,
    LoadingOverlay,
} from '@mantine/core'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import { IconRefresh } from '@tabler/icons-react'

import { getLogs } from '../api'

export default function Logs() {
    const queryClient = useQueryClient()
    const [lineCount, setLineCount] = useState<number>(100)

    const { data: logsData, isLoading, isFetching } = useQuery({
        queryKey: ['logs', lineCount],
        queryFn: () => getLogs(lineCount),
    })

    const handleRefresh = () => {
        queryClient.invalidateQueries({ queryKey: ['logs'] })
    }

    return (
        <Container size="lg">
            <Stack gap="xl">
                {/* Page Title */}
                <Group justify="space-between">
                    <div>
                        <Title order={2} mb={4}>Application Logs</Title>
                        <Text c="dimmed">View recent application log entries</Text>
                    </div>
                    <Group>
                        <NumberInput
                            value={lineCount}
                            onChange={(value) => setLineCount(Number(value) || 50)}
                            min={10}
                            max={500}
                            step={50}
                            w={100}
                            size="sm"
                        />
                        <Button
                            leftSection={<IconRefresh size={16} />}
                            onClick={handleRefresh}
                            loading={isFetching}
                            variant="light"
                        >
                            Refresh
                        </Button>
                    </Group>
                </Group>

                {/* Logs Card */}
                <Card shadow="sm" padding="lg" radius="md" withBorder pos="relative">
                    <LoadingOverlay visible={isLoading} />

                    <Text size="sm" c="dimmed" mb="md">
                        Showing {logsData?.logs?.length || 0} of {logsData?.total_lines || 0} total lines
                    </Text>

                    <ScrollArea h={600} type="auto">
                        <Code block style={{ whiteSpace: 'pre-wrap', fontSize: '12px' }}>
                            {logsData?.logs?.join('\n') || 'No logs available'}
                        </Code>
                    </ScrollArea>
                </Card>
            </Stack>
        </Container>
    )
}
