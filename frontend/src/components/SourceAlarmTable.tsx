import { useRef, useMemo, memo, useState, useEffect, useTransition } from 'react'
import {
    ScrollArea,
    Badge,
    Text,
    Button,
    Card,
    Box,
    Checkbox,
    TextInput,
    ActionIcon,
} from '@mantine/core'
import {
    useReactTable,
    getCoreRowModel,
    getSortedRowModel,
    getFilteredRowModel,
    flexRender,
    createColumnHelper,
    type SortingState,
    type ColumnFiltersState,
    type Header,
    type Table,
    type RowSelectionState,
} from '@tanstack/react-table'
import { useVirtualizer } from '@tanstack/react-virtual'
import {
    IconChevronUp,
    IconChevronDown,
    IconSelector,
    IconX
} from '@tabler/icons-react'
import dayjs from 'dayjs'
import type { AlarmAdjustment } from '../api'

interface SourceAlarmTableProps {
    data: AlarmAdjustment[]
    onAdjust: (alarm: AlarmAdjustment) => void
    rowSelection: RowSelectionState
    setRowSelection: React.Dispatch<React.SetStateAction<RowSelectionState>>
}

const columnHelper = createColumnHelper<AlarmAdjustment>()

// Instant Input Component with local state for responsiveness
function InstantInput({
    value: initialValue,
    onChange,
    ...props
}: {
    value: string | number
    onChange: (value: string | number) => void
} & Omit<React.ComponentProps<typeof TextInput>, 'onChange'>) {
    const [value, setValue] = useState(initialValue)

    useEffect(() => {
        setValue(initialValue)
    }, [initialValue])

    const handleChange = (val: string) => {
        setValue(val)
        onChange(val)
    }

    return (
        <TextInput
            {...props}
            value={value}
            onChange={e => handleChange(e.target.value)}
        />
    )
}

// Header Cell Component defined outside to preserve focus
const HeaderCell = ({ header }: { header: Header<AlarmAdjustment, unknown> }) => {
    const canSort = header.column.getCanSort()
    const canFilter = header.column.getCanFilter()
    const isResizing = header.column.getIsResizing()

    return (
        <Box
            key={header.id}
            style={{
                width: header.getSize(),
                minWidth: header.getSize(),
                position: 'relative',
                flexShrink: 0,
                backgroundColor: 'var(--mantine-color-body)',
                borderRight: '1px solid var(--mantine-color-default-border)',
            }}
        >
            <Box p="xs" style={{ display: 'flex', flexDirection: 'column', gap: '4px' }}>
                <Box
                    onClick={header.column.getToggleSortingHandler()}
                    style={{
                        cursor: canSort ? 'pointer' : 'default',
                        display: 'flex',
                        alignItems: 'center',
                        justifyContent: 'space-between',
                        userSelect: 'none'
                    }}
                >
                    <Box style={{ display: 'flex', alignItems: 'center', justifyContent: 'center', width: '100%' }}>
                        {flexRender(header.column.columnDef.header, header.getContext())}
                    </Box>
                    {canSort && (
                        <Box ml={4}>
                            {{
                                asc: <IconChevronUp size={14} />,
                                desc: <IconChevronDown size={14} />,
                            }[header.column.getIsSorted() as string] ?? <IconSelector size={14} style={{ opacity: 0.3 }} />}
                        </Box>
                    )}
                </Box>

                {canFilter && (
                    <InstantInput
                        size="xs"
                        variant="filled"
                        placeholder="Filter..."
                        value={(header.column.getFilterValue() ?? '') as string}
                        onChange={value => header.column.setFilterValue(value)}
                        rightSection={
                            header.column.getFilterValue() ? (
                                <ActionIcon
                                    size="xs"
                                    variant="subtle"
                                    color="gray"
                                    onClick={() => header.column.setFilterValue('')}
                                >
                                    <IconX size={10} />
                                </ActionIcon>
                            ) : null
                        }
                    />
                )}
            </Box>

            {header.column.getCanResize() && (
                <Box
                    onMouseDown={header.getResizeHandler()}
                    onTouchStart={header.getResizeHandler()}
                    style={{
                        position: 'absolute',
                        right: 0,
                        top: 0,
                        height: '100%',
                        width: '4px',
                        cursor: 'col-resize',
                        userSelect: 'none',
                        touchAction: 'none',
                        backgroundColor: isResizing ? 'var(--mantine-color-blue-filled)' : 'transparent',
                        zIndex: 1
                    }}
                />
            )}
        </Box>
    )
}

const TableHeader = ({ table }: { table: Table<AlarmAdjustment> }) => (
    <Box
        style={{
            display: 'flex',
            position: 'sticky',
            top: 0,
            zIndex: 2,
            backgroundColor: 'var(--mantine-color-body)',
            borderBottom: '1px solid var(--mantine-color-default-border)',
            width: table.getTotalSize(),
            fontSize: '14px',
        }}
    >
        {table.getHeaderGroups()[0].headers.map(header => (
            <HeaderCell key={header.id} header={header} />
        ))}
    </Box>
)

const SourceAlarmTable = memo(function SourceAlarmTable({ data, onAdjust, rowSelection, setRowSelection }: SourceAlarmTableProps) {
    const parentRef = useRef<HTMLDivElement>(null)
    const [sorting, setSorting] = useState<SortingState>([])
    const [columnFilters, setColumnFilters] = useState<ColumnFiltersState>([])
    const [, startTransition] = useTransition()

    const columns = useMemo(() => [
        {
            id: 'select',
            header: ({ table }: { table: any }) => (
                <Checkbox
                    checked={table.getIsAllPageRowsSelected()}
                    indeterminate={table.getIsSomePageRowsSelected()}
                    onChange={table.getToggleAllPageRowsSelectedHandler()}
                    aria-label="Select all"
                />
            ),
            cell: ({ row }: { row: any }) => (
                <div className="px-1">
                    <Checkbox
                        checked={row.getIsSelected()}
                        disabled={!row.getCanSelect()}
                        onChange={row.getToggleSelectedHandler()}
                        aria-label="Select row"
                    />
                </div>
            ),
            size: 40,
            enableSorting: false,
            enableColumnFilter: false,
            enableResizing: false,
        },
        columnHelper.accessor('id', {
            header: 'ID',
            size: 100,
            filterFn: 'includesString',
            cell: info => <Badge variant="light">{info.getValue()}</Badge>,
        }),
        columnHelper.accessor('station_nr', {
            header: 'Station',
            size: 120,
            filterFn: 'includesString',
        }),
        columnHelper.accessor('alarm_code', {
            header: 'Code',
            size: 100,
            filterFn: 'includesString',
        }),
        columnHelper.accessor('error_type', {
            header: 'Type',
            size: 140,
            cell: info => {
                const val = info.getValue()
                if (val === undefined) return null
                const isStopping = val === 0 || val === 1 || val === "0" || val === "1"
                return (
                    <Badge color={isStopping ? "red" : "gray"} variant="light">
                        {isStopping ? "Stopping" : "Non-Stopping"}
                    </Badge>
                )
            }
        }),
        columnHelper.accessor('description', {
            header: 'Description',
            size: 300,
            cell: info => (
                <Text truncate size="sm" title={info.getValue()}>
                    {info.getValue()}
                </Text>
            ),
        }),
        columnHelper.accessor('time_on', {
            header: 'Time On',
            size: 160,
            cell: info => info.getValue() ? dayjs(info.getValue()).format('YYYY-MM-DD HH:mm:ss') : '-'
        }),
        columnHelper.accessor('time_off', {
            header: 'Time Off',
            size: 160,
            cell: info => info.getValue() ? dayjs(info.getValue()).format('YYYY-MM-DD HH:mm:ss') : '-'
        }),
        columnHelper.display({
            id: 'actions',
            header: 'Action',
            size: 100,
            enableSorting: false,
            enableColumnFilter: false,
            enableResizing: false,
            cell: ({ row }) => (
                <Button
                    size="xs"
                    variant="light"
                    onClick={() => onAdjust(row.original)}
                    fullWidth
                >
                    Adjust
                </Button>
            ),
        }),
    ], [onAdjust])

    const table = useReactTable({
        data,
        columns,
        state: {
            sorting,
            columnFilters,
            rowSelection,
        },
        onSortingChange: setSorting,
        onColumnFiltersChange: (updaterOrValue) => {
            startTransition(() => {
                setColumnFilters(updaterOrValue)
            })
        },
        enableRowSelection: true,
        onRowSelectionChange: setRowSelection,
        getRowId: row => row.id.toString(),
        getCoreRowModel: getCoreRowModel(),
        getSortedRowModel: getSortedRowModel(),
        getFilteredRowModel: getFilteredRowModel(),
        enableColumnResizing: true,
        columnResizeMode: 'onChange',
    })

    const { rows } = table.getRowModel()

    const virtualizer = useVirtualizer({
        count: rows.length,
        getScrollElement: () => parentRef.current,
        estimateSize: () => 50,
        overscan: 10,
    })

    const virtualItems = virtualizer.getVirtualItems()
    const totalSize = virtualizer.getTotalSize()

    return (
        <Card shadow="sm" padding={0} radius="md" withBorder>
            <ScrollArea h={600} viewportRef={parentRef}>
                <Box style={{ width: table.getTotalSize(), minWidth: '100%' }}>
                    <TableHeader table={table} />
                    <Box style={{ height: totalSize, position: 'relative' }}>
                        {virtualItems.map((virtualRow) => {
                            const row = rows[virtualRow.index] as any
                            return (
                                <Box
                                    key={row.id}
                                    style={{
                                        position: 'absolute',
                                        top: 0,
                                        left: 0,
                                        width: '100%',
                                        height: `${virtualRow.size}px`,
                                        transform: `translateY(${virtualRow.start}px)`,
                                        display: 'flex',
                                        alignItems: 'center',
                                        borderBottom: '1px solid var(--mantine-color-default-border)',
                                        backgroundColor: virtualRow.index % 2 === 0 ? undefined : 'var(--mantine-color-dark-6)',
                                    }}
                                    className="hover-row"
                                >
                                    {row.getVisibleCells().map((cell: any) => (
                                        <Box
                                            key={cell.id}
                                            style={{
                                                width: cell.column.getSize(),
                                                minWidth: cell.column.getSize(), // Critical for resizing
                                                padding: '4px 8px', // Slightly reduced padding for compactness
                                                flexShrink: 0,
                                                overflow: 'hidden', // Add overflow handling
                                                textOverflow: 'ellipsis',
                                                whiteSpace: 'nowrap'
                                            }}
                                        >
                                            {flexRender(cell.column.columnDef.cell, cell.getContext())}
                                        </Box>
                                    ))}
                                </Box>
                            )
                        })}
                    </Box>
                </Box>
                {data.length === 0 && (
                    <Text ta="center" c="dimmed" py="xl">No data</Text>
                )}
            </ScrollArea>
        </Card>
    )
})

export default SourceAlarmTable
