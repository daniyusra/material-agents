export type PlaceType = 'material' | 'condition' | 'result' | 'unknown'

export interface Property {
  key: string
  xsiType: string
  value: string
  units?: string
  formatString?: string
  description?: string
}

export interface Place {
  id: string
  name: string
  description: string
  placeType: PlaceType
  templateId?: string
  properties: Property[]
  instanceValues: Property[]
  insertionUri?: string
}

export interface Transition {
  id: string
  name: string
  description: string
  instructionId?: string
  firedAt?: string
  lifecycle?: 'schedule' | 'start' | 'complete'
}

export interface Arc {
  id: string
  source: string
  target: string
}

export interface PetriNet {
  id: string
  methodId: string
  methodName: string
  places: Place[]
  transitions: Transition[]
  arcs: Arc[]
}

export interface MaimlDocument {
  uuid: string
  name: string
  date?: string
  instrument: string
  vendor: string
  owner: string
  petriNets: PetriNet[]
}
