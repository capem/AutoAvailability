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
} from '@mantine/core'
import { useQuery } from '@tanstack/react-query'
import {
    IconDatabase,
    IconMail,
    IconFolder,
    IconCheck,
} from '@tabler/icons-react'

import { getSystemStatus } from '../api'

export default function Settings() {
    const { data: systemStatus, isLoading } = useQuery({
        queryKey: ['systemStatus'],
        queryFn: getSystemStatus,
        refetchInterval: 10000,
    })

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
                    <Text c="dimmed">View system configuration and health status</Text>
                </div>

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
                        {systemStatus?.components.map((component) => {
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
