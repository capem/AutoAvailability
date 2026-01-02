import { useState } from 'react'
import {
    Container,
    Title,
    Text,
    Group,
    Stack,
    Breadcrumbs,
    Anchor,
    ActionIcon,
    Tooltip,
    Button,
    MultiSelect,
    Paper,
    Divider,
    Transition,
} from '@mantine/core'
import { MonthPickerInput } from '@mantine/dates'
import { DataTable, type DataTableSortStatus } from 'mantine-datatable'
import { useQuery, useQueryClient } from '@tanstack/react-query'
import {
    IconFolder,
    IconFile,
    IconDownload,
    IconRefresh,
    IconArrowUp,
    IconHome,
    IconSearch,
    IconFilter,
    IconFileTypeCsv,
    IconFileTypePdf,
} from '@tabler/icons-react'
import dayjs from 'dayjs'

import { listFiles, searchFiles, getDownloadUrl, downloadZip, type FileItem } from '../api'

export default function FileManager() {
    const queryClient = useQueryClient()
    const [currentPath, setCurrentPath] = useState('')

    // Sorting state
    const [sortStatus, setSortStatus] = useState<DataTableSortStatus<FileItem>>({
        columnAccessor: 'type',
        direction: 'asc',
    })

    // Selection state
    const [selectedRecords, setSelectedRecords] = useState<FileItem[]>([])
    const [downloadingZip, setDownloadingZip] = useState(false)

    // Advanced Filters (Multi-Select)
    const [typeFilters, setTypeFilters] = useState<string[]>([])
    const [dateFilters, setDateFilters] = useState<Date[]>([])

    // Derived state for search mode
    const isSearching = typeFilters.length > 0 || dateFilters.length > 0

    const { data: files, isLoading, isError } = useQuery({
        queryKey: ['files', currentPath, typeFilters, dateFilters],
        queryFn: () => {
            if (isSearching) {
                return searchFiles({
                    months: dateFilters.map(d => dayjs(d).format('YYYY-MM')),
                    types: typeFilters
                })
            }
            return listFiles(currentPath)
        },
        placeholderData: (previousData) => previousData,
        staleTime: 1000 * 60,
    })

    const handleNavigate = (path: string) => {
        setCurrentPath(path)
        setSelectedRecords([])
        setSortStatus({ columnAccessor: 'type', direction: 'asc' })
    }

    const handleDownload = (file: FileItem) => {
        const url = getDownloadUrl(file.path)
        const link = document.createElement('a')
        link.href = url
        link.download = file.name
        document.body.appendChild(link)
        link.click()
        document.body.removeChild(link)
    }

    const handleBulkDownload = async () => {
        if (selectedRecords.length === 0) return

        try {
            setDownloadingZip(true)
            const paths = selectedRecords.map(f => f.path)
            await downloadZip(paths)
            setSelectedRecords([])
        } catch (err) {
            console.error(err)
        } finally {
            setDownloadingZip(false)
        }
    }

    const getFileType = (file: FileItem) => {
        if (file.type === 'directory') return 'Folder'
        const ext = file.name.split('.').pop()?.toUpperCase()
        return ext ? `${ext} File` : 'File'
    }

    const getFileIcon = (file: FileItem) => {
        if (file.type === 'directory') return <IconFolder size={20} color="var(--mantine-color-blue-filled)" />
        const ext = file.name.split('.').pop()?.toLowerCase()
        if (ext === 'csv') return <IconFileTypeCsv size={20} color="var(--mantine-color-grape-6)" />
        if (ext === 'pdf') return <IconFileTypePdf size={20} color="var(--mantine-color-red-6)" />
        return <IconFile size={20} color="var(--mantine-color-gray-5)" />
    }

    // Breadcrumbs
    const breadcrumbItems = isSearching
        ? [{ title: 'Home', path: '' }, { title: 'Filter Results', path: '#' }]
        : [
            { title: 'Home', path: '' },
            ...currentPath.split('/').filter(Boolean).map((segment, index, array) => ({
                title: segment,
                path: array.slice(0, index + 1).join('/'),
            }))
        ]

    const breadcrumbs = breadcrumbItems.map((item, index) => (
        <Anchor
            key={item.path}
            size="sm"
            onClick={() => handleNavigate(item.path)}
            c={index === breadcrumbItems.length - 1 ? 'dimmed' : 'blue'}
            fw={index === breadcrumbItems.length - 1 ? 600 : 400}
            style={{ cursor: 'pointer', display: 'flex', alignItems: 'center', gap: 6 }}
        >
            {index === 0 && <IconHome size={16} />}
            {item.title}
        </Anchor>
    ))

    // Client-side Sort
    const sortedRecords = [...(files || [])].sort((a, b) => {
        const { columnAccessor, direction } = sortStatus
        const modifier = direction === 'asc' ? 1 : -1

        if (a.type !== b.type) return a.type === 'directory' ? -1 : 1

        if (columnAccessor === 'name') return a.name.localeCompare(b.name) * modifier
        if (columnAccessor === 'size') return (a.size - b.size) * modifier
        if (columnAccessor === 'mtime') return (new Date(a.mtime).getTime() - new Date(b.mtime).getTime()) * modifier
        if (columnAccessor === 'type') {
            const extA = a.name.split('.').pop()?.toLowerCase() || ''
            const extB = b.name.split('.').pop()?.toLowerCase() || ''
            return extA.localeCompare(extB) * modifier
        }

        return 0
    })

    const formatSize = (bytes: number) => {
        if (bytes === 0) return '0 B'
        const k = 1024
        const sizes = ['B', 'KB', 'MB', 'GB', 'TB']
        const i = Math.floor(Math.log(bytes) / Math.log(k))
        return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
    }

    return (
        <Container size="xl" h="100%" py="md">
            <Stack gap="lg" h="100%">

                {/* Header Section */}
                <Group justify="space-between" align="flex-end">
                    <div>
                        <Title order={2} fw={700}>Files</Title>
                        <Text c="dimmed" size="sm">Manage your data exports and results</Text>
                    </div>
                </Group>

                <Paper shadow="sm" radius="md" withBorder p="md" flex={1} style={{ display: 'flex', flexDirection: 'column' }}>

                    {/* Unified Control Bar (Multi-Select Focus) */}
                    <Stack gap="sm" mb="md">
                        <Group gap="sm" grow align="flex-start">
                            <MultiSelect
                                placeholder="Select File Types..."
                                data={['results', 'CNT', 'DIN', 'GRD', 'MET', 'SUM', 'TUR']}
                                value={typeFilters}
                                onChange={setTypeFilters}
                                leftSection={<IconFilter size={16} />}
                                clearable
                                searchable
                                hidePickedOptions
                            />

                            <MonthPickerInput
                                type="multiple"
                                placeholder="Select Periods..."
                                value={dateFilters}
                                onChange={(dates) => setDateFilters(dates as unknown as Date[])}
                                leftSection={<IconSearch size={16} />}
                                clearable
                                valueFormat="MMM YYYY"
                            />

                            <ActionIcon
                                variant="light"
                                size="input-sm"
                                radius="md"
                                onClick={() => queryClient.invalidateQueries({ queryKey: ['files'] })}
                                loading={isLoading}
                                aria-label="Refresh"
                                style={{ flexGrow: 0 }}
                            >
                                <IconRefresh size={18} />
                            </ActionIcon>
                        </Group>

                        <Divider my="xs" />

                        {/* Navigation Bar */}
                        <Group justify="space-between" h={36} align="center">
                            <Group gap="xs">
                                {currentPath && !isSearching && (
                                    <ActionIcon
                                        variant="subtle"
                                        size="sm"
                                        onClick={() => {
                                            const segments = currentPath.split('/')
                                            segments.pop()
                                            handleNavigate(segments.join('/'))
                                        }}
                                        title="Go Up"
                                    >
                                        <IconArrowUp size={16} />
                                    </ActionIcon>
                                )}
                                <Breadcrumbs separator="/" separatorMargin="xs">{breadcrumbs}</Breadcrumbs>
                            </Group>

                            {selectedRecords.length > 0 && (
                                <Transition mounted={selectedRecords.length > 0} transition="slide-up" duration={200} timingFunction="ease">
                                    {(styles) => (
                                        <Button
                                            style={styles}
                                            size="xs"
                                            variant="gradient"
                                            gradient={{ from: 'blue', to: 'cyan' }}
                                            leftSection={<IconDownload size={14} />}
                                            loading={downloadingZip}
                                            onClick={handleBulkDownload}
                                            radius="xl"
                                        >
                                            Download Selected ({selectedRecords.length})
                                        </Button>
                                    )}
                                </Transition>
                            )}
                        </Group>
                    </Stack>

                    {/* Content Table */}
                    <div style={{ flex: 1, display: 'flex', flexDirection: 'column' }}>
                        {isError ? (
                            <div style={{ padding: 40, textAlign: 'center' }}>
                                <Text c="red" size="sm">Unable to load files. Please try again.</Text>
                            </div>
                        ) : (
                            <DataTable
                                withTableBorder={false}
                                borderRadius="sm"
                                verticalSpacing="xs"
                                striped
                                highlightOnHover
                                idAccessor="path"
                                records={sortedRecords}
                                sortStatus={sortStatus}
                                onSortStatusChange={setSortStatus}
                                columns={[
                                    {
                                        accessor: 'name',
                                        title: 'Name',
                                        sortable: true,
                                        width: '40%',
                                        render: (record) => (
                                            <Group gap="xs" wrap="nowrap">
                                                {getFileIcon(record)}
                                                <Text
                                                    size="sm"
                                                    fw={500}
                                                    style={{ cursor: record.type === 'directory' ? 'pointer' : 'default' }}
                                                    onClick={() => record.type === 'directory' && handleNavigate(record.path)}
                                                >
                                                    {record.name}
                                                </Text>
                                            </Group>
                                        ),
                                    },
                                    {
                                        accessor: 'type',
                                        title: 'Type',
                                        sortable: true,
                                        render: (record) => (
                                            <Text size="sm" c="dimmed">{getFileType(record)}</Text>
                                        ),
                                    },
                                    {
                                        accessor: 'size',
                                        title: 'Size',
                                        sortable: true,
                                        render: (record) => (
                                            <Text size="sm" c="dimmed" style={{ fontFamily: 'monospace' }}>
                                                {record.type === 'file' ? formatSize(record.size) : '-'}
                                            </Text>
                                        ),
                                    },
                                    {
                                        accessor: 'mtime',
                                        title: 'Last Modified',
                                        sortable: true,
                                        width: 160,
                                        render: (record) => (
                                            <Text size="sm" c="dimmed">
                                                {dayjs(record.mtime).format('MMM D, YYYY HH:mm')}
                                            </Text>
                                        ),
                                    },
                                    {
                                        accessor: 'actions',
                                        title: '',
                                        width: 50,
                                        textAlign: 'right',
                                        render: (record) => record.type === 'file' && (
                                            <Tooltip label="Download file">
                                                <ActionIcon
                                                    variant="subtle"
                                                    color="gray"
                                                    onClick={(e) => {
                                                        e.stopPropagation()
                                                        handleDownload(record)
                                                    }}
                                                >
                                                    <IconDownload size={16} />
                                                </ActionIcon>
                                            </Tooltip>
                                        ),
                                    },
                                ]}
                                onRowClick={({ record }) => {
                                    if (record.type === 'directory') {
                                        handleNavigate(record.path)
                                    } else {
                                        const isSelected = selectedRecords.some(r => r.path === record.path)
                                        if (isSelected) {
                                            setSelectedRecords(prev => prev.filter(r => r.path !== record.path))
                                        } else {
                                            setSelectedRecords(prev => [...prev, record])
                                        }
                                    }
                                }}
                                rowStyle={() => ({ cursor: 'pointer' })}
                                selectedRecords={selectedRecords}
                                onSelectedRecordsChange={setSelectedRecords}
                                isRecordSelectable={(record) => record.type === 'file'}
                                fetching={isLoading}
                                loaderColor="blue"
                            />
                        )}

                        {!isLoading && sortedRecords.length === 0 && (
                            <div style={{ padding: 60, textAlign: 'center', opacity: 0.6 }}>
                                <IconSearch size={40} style={{ marginBottom: 10 }} />
                                <Text size="lg" fw={500}>No files found</Text>
                                <Text size="sm">
                                    {isSearching
                                        ? "Try adding more filters."
                                        : "This folder is empty."}
                                </Text>
                            </div>
                        )}
                    </div>
                </Paper>
            </Stack>
        </Container>
    )
}
