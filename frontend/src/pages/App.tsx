import React, { useEffect, useState } from 'react'

type Connector = { id: string, type: string, display_name: string, config: any }
type Destination = { id: string, type: string, display_name: string, config: any }
type Pipeline = { id: string, name: string, connector_id: string, destination_id: string, status: string }

const api = (path: string, opts?: RequestInit) => fetch(`/api${path}`, opts).then(async r => {
  if (!r.ok) throw new Error(await r.text())
  return r.json()
})

export default function App() {
  const [connectors, setConnectors] = useState<Connector[]>([])
  const [destinations, setDestinations] = useState<Destination[]>([])
  const [pipelines, setPipelines] = useState<Pipeline[]>([])
  const [creating, setCreating] = useState(false)

  // Data preview state
  const [previewPipelineId, setPreviewPipelineId] = useState<string>("")
  const [previewTable, setPreviewTable] = useState<string>("products")
  const [previewLimit, setPreviewLimit] = useState<number>(50)
  const [previewCols, setPreviewCols] = useState<string[]>([])
  const [previewRows, setPreviewRows] = useState<any[][]>([])
  const [previewLoading, setPreviewLoading] = useState(false)
  const [previewError, setPreviewError] = useState<string | null>(null)

  useEffect(() => {
    (async () => {
      setConnectors(await api('/connectors'))
      setDestinations(await api('/destinations'))
      const ps = await api('/pipelines')
      setPipelines(ps)
      // default selection for preview
      if (ps?.length && !previewPipelineId) setPreviewPipelineId(ps[0].id)
    })()
  }, [])

  const createDemoStuff = async () => {
    setCreating(true)
    try {
      const conn = await api('/connectors', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({
          type: 'rest_generic',
          display_name: 'Dummy Products',
          config: { base_url: 'https://dummyjson.com', endpoint: '/products' }
        })
      })
      const dest = await api('/destinations', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ type: 'duckdb', display_name: 'Local DuckDB', config: {} })
      })
      await api('/pipelines', {
        method: 'POST',
        headers: {'Content-Type':'application/json'},
        body: JSON.stringify({ name: 'demo_rest_to_duckdb', connector_id: conn.id, destination_id: dest.id, config: {} })
      })
      const ps = await api('/pipelines')
      setPipelines(ps)
      if (ps?.length) setPreviewPipelineId(ps[0].id)
    } finally {
      setCreating(false)
    }
  }

  const runPipeline = async (id: string) => {
    const res = await api(`/pipelines/${id}/run`, { method: 'POST' })
    alert(`Queued run: ${res.run_id}`)
  }

  const loadPreview = async () => {
    if (!previewPipelineId) return
    setPreviewLoading(true)
    setPreviewError(null)
    setPreviewRows([])
    setPreviewCols([])
    try {
      const q = new URLSearchParams({
        pipeline_id: previewPipelineId,
        table: previewTable || 'products',
        limit: String(previewLimit || 50),
      }).toString()
      const res = await api(`/data/preview?${q}`)
      setPreviewCols(res.columns || [])
      setPreviewRows(res.rows || [])
    } catch (e: any) {
      setPreviewError(e?.message || String(e))
    } finally {
      setPreviewLoading(false)
    }
  }

  return (
    <div style={{fontFamily:'Inter, system-ui, Arial', padding: 24, maxWidth: 1100, margin: '0 auto'}}>
      <h1>dlt GUI (starter)</h1>
      <p>Spin up a connector, destination, and pipeline, then run it. Now with a Data Preview tab.</p>

      <button disabled={creating} onClick={createDemoStuff}>
        {creating ? 'Creating…' : 'Create demo (REST → DuckDB)'}
      </button>

      <h2 style={{marginTop: 24}}>Pipelines</h2>
      <table cellPadding={8} style={{width:'100%', borderCollapse:'collapse'}}>
        <thead><tr><th align="left">Name</th><th>Connector</th><th>Destination</th><th>Status</th><th>Actions</th></tr></thead>
        <tbody>
          {pipelines.map(p => (
            <tr key={p.id} style={{borderTop:'1px solid #eee'}}>
              <td>{p.name}</td>
              <td>{p.connector_id}</td>
              <td>{p.destination_id}</td>
              <td>{p.status ?? 'idle'}</td>
              <td>
                <button onClick={() => runPipeline(p.id)}>Run</button>
                <button style={{ marginLeft: 8 }} onClick={() => { setPreviewPipelineId(p.id); setPreviewTable('products'); loadPreview(); }}>
                  Preview
                </button>
              </td>
            </tr>
          ))}
          {pipelines.length === 0 && <tr><td colSpan={5} style={{opacity:0.7}}>No pipelines yet.</td></tr>}
        </tbody>
      </table>

      <h2 style={{marginTop: 36}}>Data Preview</h2>
      <div style={{display:'grid', gridTemplateColumns:'1fr 1fr 1fr auto', gap:12, alignItems:'end', maxWidth:900}}>
        <div>
          <label style={{display:'block', fontSize:12, opacity:0.7}}>Pipeline</label>
          <select value={previewPipelineId} onChange={e => setPreviewPipelineId(e.target.value)} style={{padding:8, width:'100%'}}>
            <option value="" disabled>Select a pipeline</option>
            {pipelines.map(p => <option key={p.id} value={p.id}>{p.name}</option>)}
          </select>
        </div>
        <div>
          <label style={{display:'block', fontSize:12, opacity:0.7}}>Table</label>
          <input value={previewTable} onChange={e => setPreviewTable(e.target.value)} placeholder="products" style={{padding:8, width:'100%'}}/>
        </div>
        <div>
          <label style={{display:'block', fontSize:12, opacity:0.7}}>Limit</label>
          <input type="number" value={previewLimit} onChange={e => setPreviewLimit(parseInt(e.target.value || '50', 10))} min={1} max={500} style={{padding:8, width:'100%'}}/>
        </div>
        <div>
          <button onClick={loadPreview} disabled={!previewPipelineId || previewLoading}>
            {previewLoading ? 'Loading…' : 'Load preview'}
          </button>
        </div>
      </div>

      {previewError && <p style={{color:'crimson', marginTop:12}}>Error: {previewError}</p>}

      {previewCols.length > 0 && (
        <div style={{marginTop:16, overflow:'auto', border:'1px solid #eee', borderRadius:8}}>
          <table cellPadding={8} style={{borderCollapse:'collapse', minWidth:800, width:'100%'}}>
            <thead>
              <tr style={{background:'#fafafa', borderBottom:'1px solid #eee'}}>
                {previewCols.map(c => <th key={c} align="left">{c}</th>)}
              </tr>
            </thead>
            <tbody>
              {previewRows.map((row, i) => (
                <tr key={i} style={{borderTop:'1px solid #f3f3f3'}}>
                  {row.map((cell, j) => (
                    <td key={j} style={{verticalAlign:'top', whiteSpace:'pre-wrap'}}>{typeof cell === 'object' ? JSON.stringify(cell) : String(cell)}</td>
                  ))}
                </tr>
              ))}
              {previewRows.length === 0 && (
                <tr><td colSpan={previewCols.length} style={{opacity:0.7}}>No rows returned.</td></tr>
              )}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}
