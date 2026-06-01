import { createContext, createElement, useContext, type ReactNode } from "react";
import type { ContractRouter } from "@onveloz/pulse-contract";
import type { Client } from "@onveloz/pulse-client";

interface PulseContextValue {
  client: Client<ContractRouter>;
}

const PulseContext = createContext<PulseContextValue | null>(null);

export interface PulseProviderProps<C extends ContractRouter> {
  client: Client<C>;
  children: ReactNode;
}

/**
 * Provides the Pulse client to the React tree. In M2 this also owns the SSE
 * connection and feeds reactive query deltas into the TanStack Query cache.
 */
export function PulseProvider<C extends ContractRouter>({
  client,
  children,
}: PulseProviderProps<C>) {
  return createElement(
    PulseContext.Provider,
    { value: { client: client as Client<ContractRouter> } },
    children,
  );
}

/** Access the typed Pulse client from context. */
export function usePulseClient<C extends ContractRouter>(): Client<C> {
  const ctx = useContext(PulseContext);
  if (!ctx) throw new Error("usePulseClient must be used within <PulseProvider>");
  return ctx.client as Client<C>;
}
