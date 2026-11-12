import type { KeyhiveAccess, KeyhiveAgentEvents } from '#wasm-bodge/bindings';
// `/slim` loads no Wasm. Every ESM entry of `@keyhive/keyhive` except
// `/debug` re-exports the same bindings module, so these are the classes of
// the app's keyhive whichever of those entries the app imports. The CommonJS
// entries load a different bindings module, so loading one package as ESM
// and the other as CommonJS gives two keyhive modules.
import {
    Archive,
    ContactCard,
    DocumentId,
    GroupId,
    Identifier,
    IndividualId,
    type Keyhive,
} from '@keyhive/keyhive/slim';

/**
 * Wrap `keyhive` as the byte-level `KeyhiveAccess` that `KeyhiveSync.create`
 * expects. Only bytes cross the boundary so the protocol and the keyhive can
 * be in separate Wasm modules. `keyhive` must come from an ESM entry of
 * `@keyhive/keyhive` other than `/debug`, because the adapter constructs that
 * module's classes.
 */
export function keyhiveAccess(keyhive: Keyhive): KeyhiveAccess {
    const ownId = keyhive.id;
    const selfId = ownId.toBytes();
    ownId.free();
    return {
        id: () => selfId,

        contactCard: async () => freeAfter(await keyhive.getExistingContactCard(), card => card.toBytes()),

        totalOps: async () => freeAfter(await keyhive.stats(), stats => stats.totalOps),

        pendingEventHashes: () => keyhive.pendingEventHashes(),

        isKnownAgent: async id => {
            if (bytesEqual(id, selfId)) return true;
            return (
                (await freeAfter(new IndividualId(id), i => keyhive.hasIndividual(i))) ||
                (await freeAfter(new DocumentId(id), d => keyhive.hasDocument(d))) ||
                (await freeAfter(new GroupId(id), g => keyhive.hasGroup(g)))
            );
        },

        eventsForAgent: async id => {
            const principal = await freeAfter(new Identifier(id), i => keyhive.getPrincipal(i));
            return freeAfter(principal, p => keyhive.eventsForAgent(p));
        },

        allAgentEvents: async () =>
            freeAfter(
                await keyhive.allAgentEvents(),
                (all): KeyhiveAgentEvents => ({
                    events: all.events,
                    membershipSources: all.membershipSources,
                    prekeySources: all.prekeySources,
                    cgkaSources: all.cgkaSources,
                    agentMembershipSources: all.agentMembershipSources,
                    agentPrekeySources: all.agentPrekeySources,
                    agentCgkaSources: all.agentCgkaSources,
                    explanations: all.explanations,
                }),
            ),

        signBytes: async payload => freeAfter(await keyhive.trySign(payload), signed => signed.toBytes()),

        ingestEvents: events => keyhive.ingestEventsBytes(events),

        // A card's bytes are its prekey operation's event bytes.
        receiveContactCard: async card => {
            const id = await freeAfter(ContactCard.fromBytes(card), c => keyhive.receiveContactCard(c));
            id.free();
            return card;
        },

        toArchive: async () => freeAfter(await keyhive.toArchive(), archive => archive.toBytes()),

        ingestArchive: async bytes =>
            freeAfter(new Archive(bytes), archive => keyhive.ingestArchive(archive)),
    };
}

/** Apply `f` to `value`, then free `value`'s Wasm memory. */
async function freeAfter<T extends { free(): void }, R>(
    value: T,
    f: (value: T) => R | Promise<R>,
): Promise<R> {
    try {
        return await f(value);
    } finally {
        value.free();
    }
}

function bytesEqual(a: Uint8Array, b: Uint8Array): boolean {
    return a.length === b.length && a.every((byte, i) => byte === b[i]);
}
