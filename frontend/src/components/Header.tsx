import { Group, Burger, Title, Badge, Text } from '@mantine/core'
import { IconWindmill } from '@tabler/icons-react'
import { useQuery } from '@tanstack/react-query'
import { getSystemStatus } from '../api'

interface HeaderProps {
    opened: boolean
    toggle: () => void
}

export default function Header({ opened, toggle }: HeaderProps) {
    const { data: systemStatus } = useQuery({
        queryKey: ['systemStatus'],
        queryFn: getSystemStatus,
        refetchInterval: 30000,
    })

    return (
        <Group h="100%" px="md" justify="space-between">
            <Group>
                <Burger opened={opened} onClick={toggle} hiddenFrom="sm" size="sm" />
                <IconWindmill size={28} color="var(--mantine-color-blue-5)" />
                <Title order={4} visibleFrom="xs">Wind Farm Data Processing</Title>
            </Group>

            <Group gap="md">
                <Badge
                    color={systemStatus?.overall === 'healthy' ? 'green' : 'yellow'}
                    variant="light"
                    size="lg"
                >
                    System: {systemStatus?.overall || 'checking...'}
                </Badge>
                <Text size="sm" c="dimmed" visibleFrom="sm">
                    {new Date().toLocaleDateString()}
                </Text>
            </Group>
        </Group>
    )
}
