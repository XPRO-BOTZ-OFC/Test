import { Boom } from '@hapi/boom';
import { exec } from 'child_process';
import * as Crypto from 'crypto';
import { once } from 'events';
import { createReadStream, createWriteStream, promises as fs } from 'fs';
import { tmpdir } from 'os';
import { join } from 'path';
import { Readable, Transform, pipeline } from 'stream';
import { promisify } from 'util';
import { URL } from 'url';
import { proto } from '../../WAProto/index.js';
import { DEFAULT_ORIGIN, MEDIA_HKDF_KEY_MAPPING, MEDIA_PATH_MAP } from '../Defaults/index.js';
import { getBinaryNodeChild, getBinaryNodeChildBuffer, jidNormalizedUser } from '../WABinary/index.js';
import { aesDecryptGCM, aesEncryptGCM, hkdf } from './crypto.js';
import { generateMessageIDV2 } from './generics.js';

const pipelineAsync = promisify(pipeline);
const getTmpFilesDirectory = () => tmpdir();

const getImageProcessingLibrary = async () => {
    const [jimp, sharp] = await Promise.all([
        import('jimp').catch(() => { }),
        import('sharp').catch(() => { })
    ]);
    if (sharp) return { sharp };
    if (jimp) return { jimp };
    throw new Boom('No image processing library available');
};

export const hkdfInfoKey = (type) => `WhatsApp ${MEDIA_HKDF_KEY_MAPPING[type]} Keys`;

export const getRawMediaUploadData = async (media, mediaType, logger) => {
    const { stream } = await getStream(media);
    logger?.debug('got stream for raw upload');
    const hasher = Crypto.createHash('sha256');
    const filePath = join(tmpdir(), mediaType + generateMessageIDV2());
    const fileWriteStream = createWriteStream(filePath);
    let fileLength = 0;
    try {
        for await (const data of stream) {
            fileLength += data.length;
            hasher.update(data);
            if (!fileWriteStream.write(data)) await once(fileWriteStream, 'drain');
        }
        fileWriteStream.end();
        await once(fileWriteStream, 'finish');
        stream.destroy();
        const fileSha256 = hasher.digest();
        return { filePath, fileSha256, fileLength };
    } catch (error) {
        fileWriteStream.destroy();
        stream.destroy();
        await fs.unlink(filePath).catch(() => {});
        throw error;
    }
};

export async function getMediaKeys(buffer, mediaType) {
    if (!buffer) throw new Boom('Cannot derive from empty media key');
    if (typeof buffer === 'string')
        buffer = Buffer.from(buffer.replace('data:;base64,', ''), 'base64');
    const expandedMediaKey = hkdf(buffer, 112, { info: hkdfInfoKey(mediaType) });
    return {
        iv: expandedMediaKey.slice(0, 16),
        cipherKey: expandedMediaKey.slice(16, 48),
        macKey: expandedMediaKey.slice(48, 80)
    };
}

const extractVideoThumb = async (path, destPath, time, size) =>
    new Promise((resolve, reject) => {
        exec(`ffmpeg -ss ${time} -i ${path} -y -vf scale=${size.width}:-1 -vframes 1 -f image2 ${destPath}`, err =>
            err ? reject(err) : resolve()
        );
    });

export const extractImageThumb = async (bufferOrFilePath, width = 32) => {
    const lib = await getImageProcessingLibrary();

    // Stream directly without buffering if Sharp is available
    if ('sharp' in lib && typeof lib.sharp?.default === 'function') {
        let input;
        if (bufferOrFilePath instanceof Readable) {
            input = bufferOrFilePath;
        } else if (Buffer.isBuffer(bufferOrFilePath)) {
            input = bufferOrFilePath;
        } else {
            input = createReadStream(bufferOrFilePath);
        }
        const transformer = lib.sharp.default(input);
        const dimensions = await transformer.metadata();
        const buffer = await transformer.resize(width).jpeg({ quality: 50 }).toBuffer();
        return {
            buffer,
            original: { width: dimensions.width, height: dimensions.height }
        };
    }

    // Fallback to Jimp (requires full buffer)
    let inputBuffer = bufferOrFilePath;
    if (!Buffer.isBuffer(inputBuffer) && !(inputBuffer instanceof Readable)) {
        inputBuffer = await fs.readFile(inputBuffer);
    }
    if (inputBuffer instanceof Readable) inputBuffer = await toBuffer(inputBuffer);
    const jimp = await lib.jimp.Jimp.read(inputBuffer);
    const dimensions = { width: jimp.width, height: jimp.height };
    const buffer = await jimp
        .resize({ w: width, mode: lib.jimp.ResizeStrategy.BILINEAR })
        .getBuffer('image/jpeg', { quality: 50 });
    return { buffer, original: dimensions };
};

export const encodeBase64EncodedStringForUpload = b64 =>
    encodeURIComponent(b64.replace(/\+/g, '-').replace(/\//g, '_').replace(/\=+$/, ''));

export const generateProfilePicture = async (mediaUpload, dimensions) => {
    const { width: w = 640, height: h = 640 } = dimensions || {};
    const { stream } = await getStream(mediaUpload);
    const buffer = await toBuffer(stream);
    const lib = await getImageProcessingLibrary();
    let img;
    if ('sharp' in lib && typeof lib.sharp?.default === 'function') {
        img = lib.sharp.default(buffer).resize(w, h).jpeg({ quality: 50 }).toBuffer();
    } else if ('jimp' in lib && typeof lib.jimp?.Jimp === 'function') {
        const jimp = await lib.jimp.Jimp.read(buffer);
        const min = Math.min(jimp.width, jimp.height);
        const cropped = jimp.crop({ x: 0, y: 0, w: min, h: min });
        img = cropped.resize({ w, h, mode: lib.jimp.ResizeStrategy.BILINEAR }).getBuffer('image/jpeg', { quality: 50 });
    } else {
        throw new Boom('No image processing library available');
    }
    return { img: await img };
};

export const mediaMessageSHA256B64 = message => {
    const media = Object.values(message)[0];
    return media?.fileSha256 && Buffer.from(media.fileSha256).toString('base64');
};

// Streaming audio duration using music-metadata with a limited read (stops after header)
export async function getAudioDuration(buffer) {
    const musicMetadata = await import('music-metadata');
    let duration = 0;
    try {
        if (Buffer.isBuffer(buffer)) {
            const metadata = await musicMetadata.parseBuffer(buffer, undefined, { duration: true });
            duration = metadata.format.duration || 0;
        } else if (typeof buffer === 'string') {
            const metadata = await musicMetadata.parseFile(buffer, { duration: true });
            duration = metadata.format.duration || 0;
        } else {
            // For streams, create a passthrough that limits bytes read (first 2MB enough for duration)
            const limitStream = new Transform({
                transform(chunk, enc, cb) {
                    if (this.bytesRead + chunk.length > 2 * 1024 * 1024) {
                        this.destroy();
                        return cb();
                    }
                    this.bytesRead += chunk.length;
                    cb(null, chunk);
                }
            });
            limitStream.bytesRead = 0;
            const limited = buffer.pipe(limitStream);
            const metadata = await musicMetadata.parseStream(limited, undefined, { duration: true });
            duration = metadata.format.duration || 0;
        }
    } catch (e) { /* ignore */ }
    return duration;
}

// Waveform: decode only first 2 minutes, then stop
export async function getAudioWaveform(buffer, logger) {
    try {
        const duration = await getAudioDuration(buffer);
        const MAX_DURATION_SEC = 120;
        if (duration > MAX_DURATION_SEC) {
            logger?.warn(`Audio duration ${duration}s > ${MAX_DURATION_SEC}s, skipping waveform`);
            return new Uint8Array(0);
        }
        const { default: decoder } = await import('audio-decode');
        let audioData;
        if (Buffer.isBuffer(buffer)) audioData = buffer;
        else if (typeof buffer === 'string') {
            const rStream = createReadStream(buffer);
            audioData = await toBuffer(rStream);
        } else audioData = await toBuffer(buffer);
        const audioBuffer = await decoder(audioData);
        const rawData = audioBuffer.getChannelData(0);
        const samples = 64;
        const blockSize = Math.floor(rawData.length / samples);
        const filteredData = [];
        for (let i = 0; i < samples; i++) {
            const blockStart = blockSize * i;
            let sum = 0;
            for (let j = 0; j < blockSize; j++) sum += Math.abs(rawData[blockStart + j]);
            filteredData.push(sum / blockSize);
        }
        const multiplier = Math.pow(Math.max(...filteredData), -1);
        const normalizedData = filteredData.map(n => n * multiplier);
        return new Uint8Array(normalizedData.map(n => Math.floor(100 * n)));
    } catch (e) {
        logger?.debug('Failed to generate waveform: ' + e);
        return new Uint8Array(0);
    }
}

export const toReadable = buffer => {
    const readable = new Readable({ read: () => {} });
    readable.push(buffer);
    readable.push(null);
    return readable;
};

export const toBuffer = async stream => {
    const chunks = [];
    for await (const chunk of stream) chunks.push(chunk);
    stream.destroy();
    return Buffer.concat(chunks);
};

export const getStream = async (item, opts) => {
    if (Buffer.isBuffer(item)) return { stream: toReadable(item), type: 'buffer' };
    if ('stream' in item) return { stream: item.stream, type: 'readable' };
    const urlStr = item.url.toString();
    if (urlStr.startsWith('data:')) {
        const buffer = Buffer.from(urlStr.split(',')[1], 'base64');
        return { stream: toReadable(buffer), type: 'buffer' };
    }
    if (urlStr.startsWith('http://') || urlStr.startsWith('https://'))
        return { stream: await getHttpStream(item.url, opts), type: 'remote' };
    return { stream: createReadStream(item.url), type: 'file' };
};

export async function generateThumbnail(file, mediaType, options) {
    let thumbnail, originalImageDimensions;
    if (mediaType === 'image') {
        const { buffer, original } = await extractImageThumb(file);
        thumbnail = buffer.toString('base64');
        if (original.width && original.height) originalImageDimensions = original;
    } else if (mediaType === 'video') {
        const imgFilename = join(getTmpFilesDirectory(), generateMessageIDV2() + '.jpg');
        try {
            await extractVideoThumb(file, imgFilename, '00:00:00', { width: 32, height: 32 });
            const buff = await fs.readFile(imgFilename);
            thumbnail = buff.toString('base64');
            await fs.unlink(imgFilename);
        } catch (err) {
            options.logger?.debug('could not generate video thumb: ' + err);
        }
    }
    return { thumbnail, originalImageDimensions };
}

export const getHttpStream = async (url, options = {}) => {
    const response = await fetch(url.toString(), {
        dispatcher: options.dispatcher,
        method: 'GET',
        headers: options.headers
    });
    if (!response.ok)
        throw new Boom(`Failed to fetch stream from ${url}`, { statusCode: response.status, data: { url } });
    // @ts-ignore
    return response.body instanceof Readable ? response.body : Readable.fromWeb(response.body);
};

export const encryptedStream = async (media, mediaType, { logger, saveOriginalFileIfRequired, opts } = {}) => {
    const { stream, type } = await getStream(media, opts);
    logger?.debug('fetched media stream');
    const mediaKey = Crypto.randomBytes(32);
    const { cipherKey, iv, macKey } = await getMediaKeys(mediaKey, mediaType);
    const encFilePath = join(getTmpFilesDirectory(), mediaType + generateMessageIDV2() + '-enc');
    const encFileWriteStream = createWriteStream(encFilePath);
    let originalFileStream, originalFilePath;
    if (saveOriginalFileIfRequired) {
        originalFilePath = join(getTmpFilesDirectory(), mediaType + generateMessageIDV2() + '-original');
        originalFileStream = createWriteStream(originalFilePath);
    }
    let fileLength = 0;
    const aes = Crypto.createCipheriv('aes-256-cbc', cipherKey, iv);
    const hmac = Crypto.createHmac('sha256', macKey).update(iv);
    const sha256Plain = Crypto.createHash('sha256');
    const sha256Enc = Crypto.createHash('sha256');
    const onChunk = async buff => {
        sha256Enc.update(buff);
        hmac.update(buff);
        if (!encFileWriteStream.write(buff)) await once(encFileWriteStream, 'drain');
    };
    try {
        for await (const data of stream) {
            fileLength += data.length;
            if (type === 'remote' && opts?.maxContentLength && fileLength + data.length > opts.maxContentLength)
                throw new Boom(`content length exceeded when encrypting "${type}"`, { data: { media, type } });
            if (originalFileStream && !originalFileStream.write(data)) await once(originalFileStream, 'drain');
            sha256Plain.update(data);
            await onChunk(aes.update(data));
        }
        await onChunk(aes.final());
        const mac = hmac.digest().slice(0, 10);
        sha256Enc.update(mac);
        const fileSha256 = sha256Plain.digest();
        const fileEncSha256 = sha256Enc.digest();
        encFileWriteStream.write(mac);
        const encFinishPromise = once(encFileWriteStream, 'finish');
        const originalFinishPromise = originalFileStream ? once(originalFileStream, 'finish') : Promise.resolve();
        encFileWriteStream.end();
        originalFileStream?.end?.();
        stream.destroy();
        await encFinishPromise;
        await originalFinishPromise;
        logger?.debug('encrypted data successfully');
        return { mediaKey, originalFilePath, encFilePath, mac, fileEncSha256, fileSha256, fileLength };
    } catch (error) {
        encFileWriteStream.destroy();
        originalFileStream?.destroy?.();
        aes.destroy?.();
        hmac.destroy?.();
        sha256Plain.destroy?.();
        sha256Enc.destroy?.();
        stream.destroy();
        await Promise.all([
            fs.unlink(encFilePath).catch(() => {}),
            originalFilePath ? fs.unlink(originalFilePath).catch(() => {}) : Promise.resolve()
        ]);
        throw error;
    }
};

export const DEF_MEDIA_HOST = 'mmg.whatsapp.net';
const AES_CHUNK_SIZE = 16;
const toSmallestChunkSize = num => Math.floor(num / AES_CHUNK_SIZE) * AES_CHUNK_SIZE;
export const getUrlFromDirectPath = (directPath, host = DEF_MEDIA_HOST) => `https://${host}${directPath}`;
const extractHost = url => {
    if (!url) return undefined;
    try { return new URL(url).host; } catch { return undefined; }
};

export const downloadContentFromMessage = async ({ mediaKey, directPath, url }, type, opts = {}) => {
    const fallbackHost = opts.host ?? extractHost(url);
    const downloadUrl = directPath ? getUrlFromDirectPath(directPath, fallbackHost) : url;
    if (!downloadUrl) throw new Boom('No valid media URL or directPath present', { statusCode: 400 });
    const keys = await getMediaKeys(mediaKey, type);
    return downloadEncryptedContent(downloadUrl, keys, opts);
};

export const downloadEncryptedContent = async (downloadUrl, { cipherKey, iv }, { startByte, endByte, options } = {}) => {
    let bytesFetched = 0, startChunk = 0, firstBlockIsIV = false;
    if (startByte) {
        const chunk = toSmallestChunkSize(startByte || 0);
        if (chunk) {
            startChunk = chunk - AES_CHUNK_SIZE;
            bytesFetched = chunk;
            firstBlockIsIV = true;
        }
    }
    const endChunk = endByte ? toSmallestChunkSize(endByte || 0) + AES_CHUNK_SIZE : undefined;
    const headersInit = options?.headers;
    const headers = {
        ...(headersInit ? (Array.isArray(headersInit) ? Object.fromEntries(headersInit) : headersInit) : {}),
        Origin: DEFAULT_ORIGIN
    };
    if (startChunk || endChunk) {
        headers.Range = `bytes=${startChunk}-`;
        if (endChunk) headers.Range += endChunk;
    }
    const fetched = await getHttpStream(downloadUrl, { ...(options || {}), headers });
    let remainingBytes = Buffer.from([]), aes;
    const pushBytes = (bytes, push) => {
        if (startByte || endByte) {
            const start = bytesFetched >= startByte ? undefined : Math.max(startByte - bytesFetched, 0);
            const end = bytesFetched + bytes.length < endByte ? undefined : Math.max(endByte - bytesFetched, 0);
            push(bytes.slice(start, end));
            bytesFetched += bytes.length;
        } else push(bytes);
    };
    const output = new Transform({
        transform(chunk, _, callback) {
            let data = remainingBytes.length ? Buffer.concat([remainingBytes, chunk]) : chunk;
            const decryptLength = toSmallestChunkSize(data.length);
            remainingBytes = data.slice(decryptLength);
            data = data.slice(0, decryptLength);
            if (!aes) {
                let ivValue = iv;
                if (firstBlockIsIV) {
                    ivValue = data.slice(0, AES_CHUNK_SIZE);
                    data = data.slice(AES_CHUNK_SIZE);
                }
                aes = Crypto.createDecipheriv('aes-256-cbc', cipherKey, ivValue);
                if (endByte) aes.setAutoPadding(false);
            }
            try {
                pushBytes(aes.update(data), b => this.push(b));
                callback();
            } catch (error) { callback(error); }
        },
        final(callback) {
            try {
                pushBytes(aes.final(), b => this.push(b));
                callback();
            } catch (error) { callback(error); }
        }
    });
    return fetched.pipe(output, { end: true });
};

export function extensionForMediaMessage(message) {
    const getExtension = mimetype => mimetype.split(';')[0]?.split('/')[1];
    const type = Object.keys(message)[0];
    if (type === 'locationMessage' || type === 'liveLocationMessage' || type === 'productMessage') return '.jpeg';
    return getExtension(message[type].mimetype);
}

const isNodeRuntime = () =>
    typeof process !== 'undefined' && process.versions?.node && !process.versions.bun && !globalThis.Deno;

export const uploadWithNodeHttp = async ({ url, filePath, headers, timeoutMs, agent }, redirectCount = 0) => {
    if (redirectCount > 5) throw new Error('Too many redirects');
    const parsedUrl = new URL(url);
    const httpModule = parsedUrl.protocol === 'https:' ? await import('https') : await import('http');
    const fileStats = await fs.stat(filePath);
    const fileSize = fileStats.size;
    return new Promise((resolve, reject) => {
        const req = httpModule.request({
            hostname: parsedUrl.hostname,
            port: parsedUrl.port || (parsedUrl.protocol === 'https:' ? 443 : 80),
            path: parsedUrl.pathname + parsedUrl.search,
            method: 'POST',
            headers: { ...headers, 'Content-Length': fileSize },
            agent,
            timeout: timeoutMs
        }, res => {
            if (res.statusCode >= 300 && res.statusCode < 400 && res.headers.location) {
                res.resume();
                const newUrl = new URL(res.headers.location, url).toString();
                resolve(uploadWithNodeHttp({ url: newUrl, filePath, headers, timeoutMs, agent }, redirectCount + 1));
                return;
            }
            let body = '';
            res.on('data', chunk => (body += chunk));
            res.on('end', () => { try { resolve(JSON.parse(body)); } catch { resolve(undefined); } });
        });
        req.on('error', reject);
        req.on('timeout', () => { req.destroy(); reject(new Error('Upload timeout')); });
        const stream = createReadStream(filePath);
        stream.pipe(req);
        stream.on('error', err => { req.destroy(); reject(err); });
    });
};

const uploadWithFetch = async ({ url, filePath, headers, timeoutMs, agent }) => {
    const nodeStream = createReadStream(filePath);
    const webStream = Readable.toWeb(nodeStream);
    const dispatcher = typeof agent?.dispatch === 'function' ? agent : undefined;
    const response = await fetch(url, {
        ...(dispatcher ? { dispatcher } : {}),
        method: 'POST',
        body: webStream,
        headers,
        duplex: 'half',
        signal: timeoutMs ? AbortSignal.timeout(timeoutMs) : undefined
    });
    try { return await response.json(); } catch { return undefined; }
};

const uploadMedia = async (params, logger) => {
    if (isNodeRuntime()) {
        logger?.debug('Using Node.js https module for upload');
        return uploadWithNodeHttp(params);
    }
    logger?.debug('Using web-standard Fetch API for upload');
    return uploadWithFetch(params);
};

export const getWAUploadToServer = ({ customUploadHosts, fetchAgent, logger, options }, refreshMediaConn) => {
    return async (filePath, { mediaType, fileEncSha256B64, timeoutMs }) => {
        let uploadInfo = await refreshMediaConn(false);
        let urls;
        const hosts = [...customUploadHosts, ...uploadInfo.hosts];
        fileEncSha256B64 = encodeBase64EncodedStringForUpload(fileEncSha256B64);
        const customHeaders = (() => {
            const hdrs = options?.headers;
            if (!hdrs) return {};
            return Array.isArray(hdrs) ? Object.fromEntries(hdrs) : hdrs;
        })();
        const headers = { ...customHeaders, 'Content-Type': 'application/octet-stream', Origin: DEFAULT_ORIGIN };
        for (const { hostname } of hosts) {
            logger.debug(`uploading to "${hostname}"`);
            const auth = encodeURIComponent(uploadInfo.auth);
            const url = `https://${hostname}${MEDIA_PATH_MAP[mediaType]}/${fileEncSha256B64}?auth=${auth}&token=${fileEncSha256B64}`;
            let result;
            try {
                result = await uploadMedia({ url, filePath, headers, timeoutMs, agent: fetchAgent }, logger);
                if (result?.url || result?.direct_path) {
                    urls = {
                        mediaUrl: result.url,
                        directPath: result.direct_path,
                        meta_hmac: result.meta_hmac,
                        fbid: result.fbid,
                        ts: result.ts
                    };
                    break;
                } else {
                    uploadInfo = await refreshMediaConn(true);
                    throw new Error(`upload failed, reason: ${JSON.stringify(result)}`);
                }
            } catch (error) {
                const isLast = hostname === hosts[uploadInfo.hosts.length - 1]?.hostname;
                logger.warn({ trace: error?.stack, uploadResult: result }, `Error uploading to ${hostname} ${isLast ? '' : ', retrying...'}`);
            }
        }
        if (!urls) throw new Boom('Media upload failed on all hosts', { statusCode: 500 });
        return urls;
    };
};

const getMediaRetryKey = mediaKey => hkdf(mediaKey, 32, { info: 'WhatsApp Media Retry Notification' });

export const encryptMediaRetryRequest = (key, mediaKey, meId) => {
    const recp = { stanzaId: key.id };
    const recpBuffer = proto.ServerErrorReceipt.encode(recp).finish();
    const iv = Crypto.randomBytes(12);
    const retryKey = getMediaRetryKey(mediaKey);
    const ciphertext = aesEncryptGCM(recpBuffer, retryKey, iv, Buffer.from(key.id));
    return {
        tag: 'receipt',
        attrs: { id: key.id, to: jidNormalizedUser(meId), type: 'server-error' },
        content: [
            { tag: 'encrypt', attrs: {}, content: [{ tag: 'enc_p', attrs: {}, content: ciphertext }, { tag: 'enc_iv', attrs: {}, content: iv }] },
            { tag: 'rmr', attrs: { jid: key.remoteJid, from_me: (!!key.fromMe).toString(), participant: key.participant || undefined } }
        ]
    };
};

export const decodeMediaRetryNode = node => {
    const rmrNode = getBinaryNodeChild(node, 'rmr');
    const event = {
        key: {
            id: node.attrs.id,
            remoteJid: rmrNode.attrs.jid,
            fromMe: rmrNode.attrs.from_me === 'true',
            participant: rmrNode.attrs.participant
        }
    };
    const errorNode = getBinaryNodeChild(node, 'error');
    if (errorNode) {
        const errorCode = +errorNode.attrs.code;
        event.error = new Boom(`Failed to re-upload media (${errorCode})`, {
            data: errorNode.attrs,
            statusCode: getStatusCodeForMediaRetry(errorCode)
        });
    } else {
        const encryptedInfoNode = getBinaryNodeChild(node, 'encrypt');
        const ciphertext = getBinaryNodeChildBuffer(encryptedInfoNode, 'enc_p');
        const iv = getBinaryNodeChildBuffer(encryptedInfoNode, 'enc_iv');
        if (ciphertext && iv) event.media = { ciphertext, iv };
        else event.error = new Boom('Failed to re-upload media (missing ciphertext)', { statusCode: 404 });
    }
    return event;
};

export const decryptMediaRetryData = ({ ciphertext, iv }, mediaKey, msgId) => {
    const retryKey = getMediaRetryKey(mediaKey);
    const plaintext = aesDecryptGCM(ciphertext, retryKey, iv, Buffer.from(msgId));
    return proto.MediaRetryNotification.decode(plaintext);
};

export const getStatusCodeForMediaRetry = code => MEDIA_RETRY_STATUS_MAP[code];
const MEDIA_RETRY_STATUS_MAP = {
    [proto.MediaRetryNotification.ResultType.SUCCESS]: 200,
    [proto.MediaRetryNotification.ResultType.DECRYPTION_ERROR]: 412,
    [proto.MediaRetryNotification.ResultType.NOT_FOUND]: 404,
    [proto.MediaRetryNotification.ResultType.GENERAL_ERROR]: 418
};
