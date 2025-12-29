import { NavLink, Stack, Text, Badge, Group, useMantineColorScheme, ActionIcon } from '@mantine/core'
import { useLocation, useNavigate } from 'react-router-dom'
import {
    IconDashboard,
    IconBell,
    IconFileText,
    IconSettings,
    IconSun,
    IconMoon,
} from '@tabler/icons-react'
import { useQuery } from '@tanstack/react-query'
import { getProcessingStatus } from '../api'

const navItems = [
    { label: 'Dashboard', icon: IconDashboard, path: '/' },
    { label: 'Alarm Adjustments', icon: IconBell, path: '/alarms' },
    { label: 'Logs', icon: IconFileText, path: '/logs' },
    { label: 'Settings', icon: IconSettings, path: '/settings' },
]

export default function Navbar() {
    const location = useLocation()
    const navigate = useNavigate()
    const { colorScheme, toggleColorScheme } = useMantineColorScheme()

    const { data: processingStatus } = useQuery({
        queryKey: ['processingStatus'],
        queryFn: getProcessingStatus,
        refetchInterval: (query) => {
            const status = query.state.data?.status
            // Poll fast when active, slow when idle
            return status === 'running' || status === 'starting' ? 2000 : 30000
        },
    })

    const getStatusColor = () => {
        if (!processingStatus) return 'gray'
        switch (processingStatus.status) {
            case 'running':
            case 'starting':
                return 'blue'
            case 'completed':
                return 'green'
            case 'error':
                return 'red'
            default:
                return 'gray'
        }
    }

    return (
        <Stack p="md" gap="xs" style={{ height: '100%' }}>
            {/* Processing Status */}
            <Group gap="xs" mb="md" p="sm" style={{
                background: 'var(--mantine-color-dark-6)',
                borderRadius: 'var(--mantine-radius-md)'
            }}>
                <Badge color={getStatusColor()} variant="dot" size="lg">
                    {processingStatus?.status || 'idle'}
                </Badge>
                {processingStatus?.step && (
                    <Text size="xs" c="dimmed">{processingStatus.step}</Text>
                )}
            </Group>

            {/* Navigation Links */}
            <Stack gap={4}>
                {navItems.map((item) => (
                    <NavLink
                        key={item.path}
                        label={item.label}
                        leftSection={<item.icon size={20} stroke={1.5} />}
                        active={location.pathname === item.path}
                        onClick={() => navigate(item.path)}
                        variant="filled"
                        style={{ borderRadius: 'var(--mantine-radius-md)' }}
                    />
                ))}
            </Stack>

            {/* Theme Toggle at Bottom */}
            <div style={{ marginTop: 'auto' }}>
                <Group justify="center" p="sm">
                    <ActionIcon
                        variant="default"
                        size="lg"
                        onClick={() => toggleColorScheme()}
                        title="Toggle color scheme"
                    >
                        {colorScheme === 'dark' ? <IconSun size={18} /> : <IconMoon size={18} />}
                    </ActionIcon>
                </Group>
            </div>
        </Stack>
    )
}
