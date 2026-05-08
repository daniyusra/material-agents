import { describe, it, expect } from 'vitest'
import semXml from './__fixtures__/sem.maiml?raw'
import { parseMaiml } from './parseMaiml'

describe('parseMaiml — SEM fixture', () => {
  const doc = parseMaiml(semXml)

  it('returns one petri net', () => {
    expect(doc.petriNets.length).toBe(1)
  })

  const net = () => doc.petriNets[0]

  it('has 4 places', () => {
    expect(net().places.length).toBe(4)
  })

  it('has 3 transitions', () => {
    expect(net().transitions.length).toBe(3)
  })

  it('has 8 arcs', () => {
    expect(net().arcs.length).toBe(8)
  })

  it('has correct place types: material×1, condition×1, result×2', () => {
    const types = net().places.map(p => p.placeType)
    expect(types.filter(t => t === 'material').length).toBe(1)
    expect(types.filter(t => t === 'condition').length).toBe(1)
    expect(types.filter(t => t === 'result').length).toBe(2)
    expect(types.filter(t => t === 'unknown').length).toBe(0)
  })

  it('condition place has >= 70 template properties', () => {
    const condPlace = net().places.find(p => p.placeType === 'condition')
    expect(condPlace).toBeDefined()
    expect(condPlace!.properties.length).toBeGreaterThanOrEqual(70)
  })

  it('arcs connect valid node ids', () => {
    const allIds = new Set([
      ...net().places.map(p => p.id),
      ...net().transitions.map(t => t.id),
    ])
    for (const arc of net().arcs) {
      expect(allIds.has(arc.source), `arc ${arc.id} source unknown`).toBe(true)
      expect(allIds.has(arc.target), `arc ${arc.id} target unknown`).toBe(true)
    }
  })

  it('eventLog: semMeasurement transition gets firedAt and complete lifecycle', () => {
    const trans = net().transitions.find(t => t.id === 'transition_semMeasurement')
    expect(trans).toBeDefined()
    expect(trans!.firedAt).toBe('2023-10-25T13:22:00+09:00')
    expect(trans!.lifecycle).toBe('complete')
  })

  it('document metadata is parsed', () => {
    expect(doc.uuid).toBe('2e09f892-c2eb-4ea5-a8c7-b1b4a0231e83')
    expect(doc.vendor).toBe('JEOL')
    expect(doc.owner).toBe('Hitosugi-Lab')
    expect(doc.instrument).toBe('scanningElectronMicroscope')
  })

  it('material place has instanceValues from <data>', () => {
    const mat = net().places.find(p => p.placeType === 'material')
    expect(mat).toBeDefined()
    expect(mat!.instanceValues.length).toBeGreaterThan(0)
    const sampleId = mat!.instanceValues.find(p => p.key === 'SampleId')
    expect(sampleId?.value).toBe('8fa67ebc-f9a6-4cfe-8433-ceee619a3943')
  })

  it('transition has name from instruction element', () => {
    const trans = net().transitions.find(t => t.id === 'transition_semMeasurement')
    expect(trans).toBeDefined()
    expect(trans!.name).toBeTruthy()
  })
})
