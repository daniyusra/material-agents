import type {
  MaimlDocument,
  PetriNet,
  Place,
  Transition,
  Arc,
  Property,
  PlaceType,
} from './types'

const ln = (el: Element): string => el.tagName.split(':').pop()!

const childrenByTag = (el: Element, localName: string): Element[] =>
  Array.from(el.children).filter(c => ln(c) === localName)

const firstChild = (el: Element, localName: string): Element | undefined =>
  Array.from(el.children).find(c => ln(c) === localName)

const textOf = (el: Element, localName: string): string =>
  firstChild(el, localName)?.textContent?.trim() ?? ''

const allDescendantsByTag = (el: Element, localName: string): Element[] =>
  Array.from(el.getElementsByTagName('*')).filter(e => ln(e) === localName)

const xsiType = (el: Element): string =>
  el.getAttribute('xsi:type') ??
  el.getAttributeNS('http://www.w3.org/2001/XMLSchema-instance', 'type') ??
  ''

function parseProperties(container: Element): Property[] {
  return childrenByTag(container, 'property').map(p => {
    const desc = textOf(p, 'description')
    const units = p.getAttribute('units')
    const formatString = p.getAttribute('formatString')
    return {
      key: p.getAttribute('key') ?? '',
      xsiType: xsiType(p),
      value: firstChild(p, 'value')?.textContent?.trim() ?? '',
      ...(units ? { units } : {}),
      ...(formatString ? { formatString } : {}),
      ...(desc ? { description: desc } : {}),
    }
  })
}

export function parseMaiml(xmlString: string): MaimlDocument {
  const xml = xmlString.replace(/^﻿/, '')
  const xmlDoc = new DOMParser().parseFromString(xml, 'text/xml')
  const root = xmlDoc.documentElement

  // --- document section ---
  const documentEl = firstChild(root, 'document')!
  const uuid = textOf(documentEl, 'uuid')
  const name = textOf(documentEl, 'name')
  const rawDate = textOf(documentEl, 'date')
  const vendorEl = firstChild(documentEl, 'vendor')
  const ownerEl = firstChild(documentEl, 'owner')
  const instrumentEl = firstChild(documentEl, 'instrument')
  const vendor = vendorEl ? textOf(vendorEl, 'name') : ''
  const owner = ownerEl ? textOf(ownerEl, 'name') : ''
  const instrument = instrumentEl ? textOf(instrumentEl, 'name') : ''

  // --- collect all templates from entire document ---
  const TEMPLATE_TAGS = new Set(['materialTemplate', 'conditionTemplate', 'resultTemplate'])

  const templateMap = new Map<
    string,
    { placeId: string; placeType: PlaceType; properties: Property[] }
  >()

  for (const el of Array.from(root.getElementsByTagName('*'))) {
    const tag = ln(el)
    if (!TEMPLATE_TAGS.has(tag)) continue
    const templateId = el.getAttribute('id') ?? ''
    if (!templateId) continue
    const placeType: PlaceType =
      tag === 'materialTemplate' ? 'material' :
      tag === 'conditionTemplate' ? 'condition' :
      'result'
    const placeRefEl = firstChild(el, 'placeRef')
    const placeId = placeRefEl?.getAttribute('ref') ?? ''
    templateMap.set(templateId, { placeId, placeType, properties: parseProperties(el) })
  }

  // reverse map: placeId → templateId
  const placeToTemplate = new Map<string, string>()
  for (const [tid, info] of templateMap) {
    if (info.placeId) placeToTemplate.set(info.placeId, tid)
  }

  // --- collect data instances: templateId → { properties, insertionUri } ---
  const instanceMap = new Map<string, { properties: Property[]; insertionUri?: string }>()
  const dataEl = firstChild(root, 'data')
  if (dataEl) {
    for (const el of Array.from(dataEl.getElementsByTagName('*'))) {
      const tag = ln(el)
      if (tag !== 'material' && tag !== 'condition' && tag !== 'result') continue
      const ref = el.getAttribute('ref')
      if (!ref) continue
      const insertionEl = firstChild(el, 'insertion')
      const rawUri = insertionEl ? textOf(insertionEl, 'uri') : undefined
      const insertionUri = rawUri ? rawUri.replace(/^\.\//, '/') : undefined
      instanceMap.set(ref, { properties: parseProperties(el), insertionUri })
    }
  }

  // --- collect instruction → transition mapping ---
  const instructionToTransition = new Map<string, string>() // instId → transId
  const transitionDisplayName = new Map<string, string>()   // transId → name from instruction

  for (const inst of Array.from(root.getElementsByTagName('*')).filter(e => ln(e) === 'instruction')) {
    const instId = inst.getAttribute('id') ?? ''
    const instName = textOf(inst, 'name')
    const transRef = firstChild(inst, 'transitionRef')
    const transId = transRef?.getAttribute('ref') ?? ''
    if (instId && transId) {
      instructionToTransition.set(instId, transId)
      transitionDisplayName.set(transId, instName)
    }
  }

  const transitionToInstruction = new Map<string, string>()
  for (const [instId, transId] of instructionToTransition) {
    transitionToInstruction.set(transId, instId)
  }

  // --- parse eventLog: transitionId → { firedAt, lifecycle } ---
  const transitionEvents = new Map<
    string,
    { firedAt: string; lifecycle: 'schedule' | 'start' | 'complete' }
  >()
  const eventLogEl = firstChild(root, 'eventLog')
  if (eventLogEl) {
    for (const event of allDescendantsByTag(eventLogEl, 'event')) {
      const instId = event.getAttribute('ref') ?? ''
      const transId = instructionToTransition.get(instId)
      if (!transId) continue
      const props = parseProperties(event)
      const firedAt = props.find(p => p.key === 'time:timestamp')?.value ?? ''
      const lc = props.find(p => p.key === 'lifecycle:transition')?.value ?? ''
      const lifecycle =
        lc === 'schedule' ? 'schedule' :
        lc === 'start' ? 'start' :
        'complete'
      transitionEvents.set(transId, { firedAt, lifecycle })
    }
  }

  // --- build PetriNets ---
  const petriNets: PetriNet[] = []
  const protocolEl = firstChild(root, 'protocol')
  if (protocolEl) {
    for (const methodEl of childrenByTag(protocolEl, 'method')) {
      const methodId = methodEl.getAttribute('id') ?? ''
      const methodName = textOf(methodEl, 'name')
      const pnmlEl = firstChild(methodEl, 'pnml')
      if (!pnmlEl) continue

      const places: Place[] = childrenByTag(pnmlEl, 'place').map(placeEl => {
        const placeId = placeEl.getAttribute('id') ?? ''
        const templateId = placeToTemplate.get(placeId)
        const info = templateId ? templateMap.get(templateId) : undefined
        return {
          id: placeId,
          name: textOf(placeEl, 'name'),
          description: textOf(placeEl, 'description'),
          placeType: info?.placeType ?? 'unknown',
          ...(templateId ? { templateId } : {}),
          properties: info?.properties ?? [],
          instanceValues: templateId ? (instanceMap.get(templateId)?.properties ?? []) : [],
          ...(templateId && instanceMap.get(templateId)?.insertionUri
            ? { insertionUri: instanceMap.get(templateId)!.insertionUri }
            : {}),
        }
      })

      const transitions: Transition[] = childrenByTag(pnmlEl, 'transition').map(transEl => {
        const transId = transEl.getAttribute('id') ?? ''
        const evInfo = transitionEvents.get(transId)
        const instructionId = transitionToInstruction.get(transId)
        return {
          id: transId,
          name: transitionDisplayName.get(transId) ?? textOf(transEl, 'name'),
          description: textOf(transEl, 'description'),
          ...(instructionId ? { instructionId } : {}),
          ...(evInfo?.firedAt ? { firedAt: evInfo.firedAt } : {}),
          ...(evInfo?.lifecycle ? { lifecycle: evInfo.lifecycle } : {}),
        }
      })

      const arcs: Arc[] = childrenByTag(pnmlEl, 'arc').map(arcEl => ({
        id: arcEl.getAttribute('id') ?? '',
        source: arcEl.getAttribute('source') ?? '',
        target: arcEl.getAttribute('target') ?? '',
      }))

      petriNets.push({
        id: pnmlEl.getAttribute('id') ?? methodId,
        methodId,
        methodName,
        places,
        transitions,
        arcs,
      })
    }
  }

  return {
    uuid,
    name,
    ...(rawDate ? { date: rawDate } : {}),
    instrument,
    vendor,
    owner,
    petriNets,
  }
}
