import React from 'react'
import { getSortButtonTitle, type TableSortDir } from '../../utils/tableSort'
import { type ColDef, type GridRow, type HiddenBucket } from './shared'

type Props = {
  hiddenCols: ColDef[]
  collapsedOpen: boolean
  addColumnMenuOpen: boolean
  addMenuHiddenCols: ColDef[]
  primaryHiddenCols: ColDef[]
  visibleCols: ColDef[]
  draggingId: string | null
  dropHint: { id: string; side: 'left' | 'right'; x: number } | null
  tableWidth: number
  visibleRows: GridRow[]
  startRow: number
  topSpace: number
  bottomSpace: number
  empty: boolean
  sortColId: ColDef['id'] | null
  sortDir?: TableSortDir
  photoPreview: { url: string; alt: string; x: number; y: number } | null
  collapsedBtnRef: React.RefObject<HTMLButtonElement>
  collapsedMenuRef: React.RefObject<HTMLDivElement>
  resizeIndicatorRef: React.RefObject<HTMLDivElement>
  headScrollRef: React.RefObject<HTMLDivElement>
  headInnerRef: React.RefObject<HTMLDivElement>
  headTableRef: React.RefObject<HTMLTableElement>
  headerRowRef: React.RefObject<HTMLTableRowElement>
  bodyScrollRef: React.RefObject<HTMLDivElement>
  bodyInnerRef: React.RefObject<HTMLDivElement>
  bodyTableRef: React.RefObject<HTMLTableElement>
  getHeaderTitleText: (c: ColDef) => string
  getRowKey: (p: GridRow, absoluteRowIndex: number) => string
  cellText: (p: GridRow, colId: ColDef['id']) => { text: string; title?: string }
  setCollapsedOpen: React.Dispatch<React.SetStateAction<boolean>>
  setAddColumnMenuOpen: React.Dispatch<React.SetStateAction<boolean>>
  onShowCol: (id: string) => void
  onHideCol: (id: string) => void
  onMoveHiddenColToBucket: (id: string, hiddenBucket: HiddenBucket) => void
  onDragStart: (e: React.DragEvent, id: string) => void
  onDragOverHeader: (e: React.DragEvent) => void
  onDrop: (e: React.DragEvent) => void
  onDragEnd: () => void
  onToggleSort: (id: string) => void
  onStartResize: (e: React.MouseEvent, id: string) => void
  onAutoSize: (id: string) => void
  queuePhotoPreview: (url: string, alt: string, clientX: number, clientY: number) => void
  movePhotoPreview: (clientX: number, clientY: number) => void
  hidePhotoPreview: () => void
}

export default function ProductsGridView(props: Props) {
  const {
    hiddenCols,
    collapsedOpen,
    addColumnMenuOpen,
    addMenuHiddenCols,
    primaryHiddenCols,
    visibleCols,
    draggingId,
    dropHint,
    tableWidth,
    visibleRows,
    startRow,
    topSpace,
    bottomSpace,
    empty,
    sortColId,
    sortDir,
    photoPreview,
    collapsedBtnRef,
    collapsedMenuRef,
    resizeIndicatorRef,
    headScrollRef,
    headInnerRef,
    headTableRef,
    headerRowRef,
    bodyScrollRef,
    bodyInnerRef,
    bodyTableRef,
    getHeaderTitleText,
    getRowKey,
    cellText,
    setCollapsedOpen,
    setAddColumnMenuOpen,
    onShowCol,
    onHideCol,
    onMoveHiddenColToBucket,
    onDragStart,
    onDragOverHeader,
    onDrop,
    onDragEnd,
    onToggleSort,
    onStartResize,
    onAutoSize,
    queuePhotoPreview,
    movePhotoPreview,
    hidePhotoPreview,
  } = props

  return (
    <div className="productsCard">
      <div className="productsTableArea">
        <div className="tableWrap" style={{ marginTop: 0, position: 'relative' }}>
          <div className="resizeIndicator" ref={resizeIndicatorRef} style={{ display: 'none' }} />
          {hiddenCols.length > 0 && (
            <div className="collapsedCorner" style={{ position: 'absolute', top: 6, right: 6, zIndex: 5 }}>
              <button
                type="button"
                className="colToggle colTogglePlus"
                ref={collapsedBtnRef}
                title="Показать скрытый столбец"
                aria-haspopup="menu"
                aria-expanded={collapsedOpen}
                onMouseDown={(e) => e.preventDefault()}
                onClick={() => {
                  if (collapsedOpen) setAddColumnMenuOpen(false)
                  setCollapsedOpen((v) => !v)
                }}
              >
                +
              </button>

              {collapsedOpen && (
                <div
                  className="collapsedMenu"
                  ref={collapsedMenuRef}
                  role="menu"
                  style={{ position: 'absolute', top: 0, right: 'calc(100% + 6px)', left: 'auto', zIndex: 6 }}
                >
                  <button
                    type="button"
                    className="collapsedMenuItem"
                    role="menuitem"
                    style={{ padding: '6px 10px', lineHeight: 1.1, display: 'flex', alignItems: 'center', gap: 8, fontWeight: 600 }}
                    onMouseDown={(e) => e.preventDefault()}
                    onClick={() => {
                      if (addMenuHiddenCols.length === 0) {
                        setAddColumnMenuOpen(false)
                        return
                      }
                      setAddColumnMenuOpen((v) => !v)
                    }}
                  >
                    <span style={{ flex: '1 1 auto', minWidth: 0 }}>Добавить столбец</span>
                    <span aria-hidden="true" style={{ fontSize: 11, opacity: 0.7 }}>{addColumnMenuOpen ? '▾' : '▸'}</span>
                  </button>

                  {addColumnMenuOpen && addMenuHiddenCols.length > 0 && (
                    <div style={{ display: 'grid', gap: 4, marginBottom: 6, padding: '2px 0 6px 10px' }}>
                      {addMenuHiddenCols.map((c) => {
                        const id = String(c.id)
                        return (
                          <div key={id} style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
                            <button
                              type="button"
                              className="collapsedMenuItem"
                              role="menuitem"
                              style={{ padding: '6px 10px', lineHeight: 1.1, flex: '1 1 auto', minWidth: 0 }}
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={() => onShowCol(id)}
                            >
                              {c.title}
                            </button>
                            <button
                              type="button"
                              className="colToggle"
                              title="Вернуть в общий список"
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={(e) => {
                                e.stopPropagation()
                                onMoveHiddenColToBucket(id, 'main')
                              }}
                            >
                              +
                            </button>
                          </div>
                        )
                      })}
                    </div>
                  )}

                  {primaryHiddenCols.map((c) => {
                    const id = String(c.id)
                    return (
                      <div key={id} style={{ display: 'flex', alignItems: 'center', gap: 8, minWidth: 0 }}>
                        <button
                          type="button"
                          className="collapsedMenuItem"
                          role="menuitem"
                          style={{ padding: '6px 10px', lineHeight: 1.1, flex: '1 1 auto', minWidth: 0 }}
                          onMouseDown={(e) => e.preventDefault()}
                          onClick={() => onShowCol(id)}
                        >
                          {c.title}
                        </button>
                        <button
                          type="button"
                          className="colToggle"
                          title="Перенести в список «Добавить столбец»"
                          onMouseDown={(e) => e.preventDefault()}
                          onClick={(e) => {
                            e.stopPropagation()
                            onMoveHiddenColToBucket(id, 'add')
                          }}
                        >
                          −
                        </button>
                      </div>
                    )
                  })}
                </div>
              )}
            </div>
          )}

          <div className="tableHeadX" ref={headScrollRef}>
            <div className="tableWrapY tableHeadInner" ref={headInnerRef} style={{ width: tableWidth }}>
              {dropHint && <div className="dropIndicator" style={{ left: dropHint.x }} />}
              <table ref={headTableRef} className="table tableFixed tableHead" style={{ width: tableWidth }}>
                <colgroup>{visibleCols.map((c) => <col key={String(c.id)} style={{ width: c.w }} />)}</colgroup>
                <thead onDragOver={onDragOverHeader} onDrop={onDrop}>
                  <tr ref={headerRowRef}>
                    {visibleCols.map((c) => {
                      const id = String(c.id)
                      const isSorted = sortColId === c.id
                      return (
                        <th
                          key={id}
                          data-col-id={id}
                          draggable
                          onDragStart={(e) => onDragStart(e, id)}
                          onDragEnd={onDragEnd}
                          onClick={() => onToggleSort(id)}
                          className={`thDraggable ${draggingId === id ? 'thDragging' : ''}`.trim()}
                          title={getSortButtonTitle(isSorted, sortDir)}
                        >
                          <div className="thInner">
                            <button
                              type="button"
                              className="colToggle"
                              onMouseDown={(e) => e.preventDefault()}
                              onClick={(e) => {
                                e.stopPropagation()
                                onHideCol(id)
                              }}
                              title="Скрыть"
                            >
                              −
                            </button>
                            <span style={{ display: 'inline-flex', alignItems: 'center', gap: 6, minWidth: 0, flex: '1 1 auto' }}>
                              <span className="thTitle" title={getHeaderTitleText(c)}>{getHeaderTitleText(c)}</span>
                              {isSorted && (
                                <span aria-hidden="true" style={{ fontSize: 10, opacity: 0.72, flex: '0 0 auto' }}>
                                  {sortDir === 'asc' ? '▲' : '▼'}
                                </span>
                              )}
                            </span>
                          </div>
                          <div
                            className="thResizer"
                            title="Изменить ширину (двойной клик — по содержимому)"
                            onClick={(e) => e.stopPropagation()}
                            onMouseDown={(e) => {
                              e.stopPropagation()
                              onStartResize(e, id)
                            }}
                            onDoubleClick={(e) => {
                              e.preventDefault()
                              e.stopPropagation()
                              onAutoSize(id)
                            }}
                          />
                        </th>
                      )
                    })}
                  </tr>
                </thead>
              </table>
            </div>
          </div>

          <div className="tableWrapX" ref={bodyScrollRef}>
            <div className="tableWrapY" ref={bodyInnerRef} style={{ width: tableWidth }}>
              <table ref={bodyTableRef} className="table tableFixed tableBody" style={{ width: tableWidth }}>
                <colgroup>{visibleCols.map((c) => <col key={String(c.id)} style={{ width: c.w }} />)}</colgroup>
                <tbody>
                  {topSpace > 0 && (
                    <tr className="spacerRow">
                      <td colSpan={visibleCols.length} style={{ height: topSpace, padding: 0, border: 'none' }} />
                    </tr>
                  )}

                  {visibleRows.map((p, rowIdx) => (
                    <tr key={getRowKey(p, startRow + rowIdx)}>
                      {visibleCols.map((c) => {
                        const id = String(c.id)
                        const { text, title } = cellText(p, c.id)
                        if (id === 'photo_url') {
                          const url = (p.photo_url && String(p.photo_url).trim()) ? String(p.photo_url).trim() : ''
                          return (
                            <td key={id}>
                              <div
                                className="photoCell"
                                onMouseEnter={(e) => {
                                  if (!url) return
                                  queuePhotoPreview(url, p.offer_id, e.clientX, e.clientY)
                                }}
                                onMouseMove={(e) => {
                                  if (!url) return
                                  movePhotoPreview(e.clientX, e.clientY)
                                }}
                                onMouseLeave={hidePhotoPreview}
                              >
                                {url ? (
                                  <>
                                    <img
                                      className="photoThumb"
                                      src={url}
                                      alt={p.offer_id}
                                      loading="lazy"
                                      onError={(e) => {
                                        hidePhotoPreview()
                                        const img = e.currentTarget
                                        img.style.display = 'none'
                                        const fb = img.parentElement?.querySelector('.photoThumbFallback') as HTMLElement | null | undefined
                                        if (fb) fb.style.display = 'flex'
                                      }}
                                    />
                                    <div className="photoThumbFallback" style={{ display: 'none' }}>Нет фото</div>
                                  </>
                                ) : (
                                  <div className="photoThumbFallback">Нет фото</div>
                                )}
                              </div>
                            </td>
                          )
                        }
                        return (
                          <td key={id}>
                            <div className="cellText" title={title ?? text}>{text}</div>
                          </td>
                        )
                      })}
                    </tr>
                  ))}

                  {bottomSpace > 0 && (
                    <tr className="spacerRow">
                      <td colSpan={visibleCols.length} style={{ height: bottomSpace, padding: 0, border: 'none' }} />
                    </tr>
                  )}

                  {empty && (
                    <tr>
                      <td colSpan={visibleCols.length} className="empty">Ничего не найдено.</td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>

          {photoPreview && (
            <div className="photoPreviewPopover" style={{ left: photoPreview.x, top: photoPreview.y }} aria-hidden="true">
              <img className="photoPreviewImage" src={photoPreview.url} alt={photoPreview.alt} loading="eager" decoding="async" />
            </div>
          )}
        </div>
      </div>
    </div>
  )
}
