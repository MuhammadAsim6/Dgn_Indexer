/**
 * Helper for fetching WASM contract info via ABCI query.
 * Used to enrich discovered contracts with missing metadata (admin, label).
 */
import { RpcClient } from '../../../rpc/client.js';
import { Root } from 'protobufjs';
import { decodeAnyWithRoot } from '../../../decode/dynamicProto.js';
import { getLogger } from '../../../utils/logger.js';

const log = getLogger('wasm/abci');

/**
 * Fetches contract info from the chain via ABCI query.
 * @param rpc - RPC client
 * @param protoRoot - Protobuf Root for encoding/decoding
 * @param address - Contract address to fetch
 * @returns Contract metadata or null if not found
 */
export async function fetchContractInfoViaAbci(
    rpc: RpcClient,
    protoRoot: Root,
    address: string
): Promise<{
    admin: string | null;
    label: string | null;
    creator: string | null;
} | null> {
    try {
        const ReqType = protoRoot.lookupType('cosmwasm.wasm.v1.QueryContractInfoRequest');
        const ResType = 'cosmwasm.wasm.v1.QueryContractInfoResponse';

        // Encode request
        const reqMsg = ReqType.create({ address });
        const reqBytes = ReqType.encode(reqMsg).finish();
        const reqHex = '0x' + Buffer.from(reqBytes).toString('hex');

        // Query ABCI
        const path = '/cosmwasm.wasm.v1.Query/ContractInfo';
        const response = await rpc.queryAbci(path, reqHex);

        if (!response || response.code !== 0 || !response.value) {
            return null;
        }

        // Decode response
        const decoded = decodeAnyWithRoot(ResType, Buffer.from(response.value, 'base64'), protoRoot) as any;
        const info = decoded?.contract_info || decoded?.contractInfo;

        if (!info) return null;

        return {
            admin: info.admin || null,
            label: info.label || null,
            creator: info.creator || null,
        };
    } catch (err: any) {
        log.debug(`[wasm] Skip contract info fetch for ${address}: ${err.message}`);
        return null;
    }
}
