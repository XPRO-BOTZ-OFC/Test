import { Boom } from '@hapi/boom';
import { randomBytes } from 'crypto';
import { promises as fs } from 'fs';
import { proto } from '../../WAProto/index.js';
import { CALL_AUDIO_PREFIX, CALL_VIDEO_PREFIX, MEDIA_KEYS, URL_REGEX, WA_DEFAULT_EPHEMERAL } from '../Defaults/index.js';
import { WAMessageStatus } from '../Types/index.js';
import { isJidGroup, isJidNewsletter, isJidStatusBroadcast, jidNormalizedUser } from '../WABinary/index.js';
import { sha256 } from './crypto.js';
import { generateMessageIDV2, getKeyAuthor, unixTimestampSeconds } from './generics.js';
import { downloadContentFromMessage, encryptedStream, generateThumbnail, getAudioDuration, getAudioWaveform, getRawMediaUploadData } from './messages-media.js';
import { shouldIncludeReportingToken } from './reporting-utils.js';

const MIMETYPE_MAP = {
    image: 'image/jpeg',
    video: 'video/mp4',
    document: 'application/pdf',
    audio: 'audio/ogg; codecs=opus',
    sticker: 'image/webp',
    'product-catalog-image': 'image/jpeg'
};

const MessageTypeProto = {
    image: proto.Message.ImageMessage,
    video: proto.Message.VideoMessage,
    audio: proto.Message.AudioMessage,
    sticker: proto.Message.StickerMessage,
    document: proto.Message.DocumentMessage
};

const ButtonType = proto.Message.ButtonsMessage.HeaderType;

export const extractUrlFromText = (text) => text.match(URL_REGEX)?.[0];

export const generateLinkPreviewIfRequired = async (text, getUrlInfo, logger) => {
    const url = extractUrlFromText(text);
    if (getUrlInfo && url) {
        try {
            return await getUrlInfo(url);
        } catch (error) {
            logger?.warn({ trace: error.stack }, 'url preview failed');
        }
    }
};

const assertColor = async (color) => {
    if (typeof color === 'number') return color > 0 ? color : 0xffffffff + Number(color) + 1;
    let hex = color.trim().replace('#', '');
    if (hex.length <= 6) hex = 'FF' + hex.padStart(6, '0');
    return parseInt(hex, 16);
};

export const prepareWAMessageMedia = async (message, options) => {
    const logger = options.logger;
    let mediaType = MEDIA_KEYS.find(k => k in message);
    if (!mediaType) throw new Boom('Invalid media type', { statusCode: 400 });

    const uploadData = { ...message, media: message[mediaType] };
    delete uploadData[mediaType];

    const cacheableKey = (typeof uploadData.media === 'object' && 'url' in uploadData.media && uploadData.media.url && options.mediaCache)
        ? mediaType + ':' + uploadData.media.url.toString()
        : null;

    if (mediaType === 'document' && !uploadData.fileName) uploadData.fileName = 'file';
    if (!uploadData.mimetype) uploadData.mimetype = MIMETYPE_MAP[mediaType];

    if (cacheableKey) {
        const cached = await options.mediaCache.get(cacheableKey);
        if (cached) {
            logger?.debug({ cacheableKey }, 'media cache hit');
            const obj = proto.Message.decode(cached);
            const key = `${mediaType}Message`;
            Object.assign(obj[key], { ...uploadData, media: undefined });
            return obj;
        }
    }

    const isNewsletter = !!(options.jid && isJidNewsletter(options.jid));
    if (isNewsletter) {
        logger?.info('Preparing raw newsletter media');
        const { filePath, fileSha256, fileLength } = await getRawMediaUploadData(uploadData.media, options.mediaTypeOverride || mediaType, logger);
        const fileSha256B64 = fileSha256.toString('base64');
        const { mediaUrl, directPath } = await options.upload(filePath, {
            fileEncSha256B64: fileSha256B64,
            mediaType,
            timeoutMs: options.mediaUploadTimeoutMs
        });
        await fs.unlink(filePath);
        const obj = proto.Message.fromObject({
            [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
                url: mediaUrl, directPath, fileSha256, fileLength,
                ...uploadData, media: undefined
            })
        });
        if (uploadData.ptv) { obj.ptvMessage = obj.videoMessage; delete obj.videoMessage; }
        if (obj.stickerMessage) obj.stickerMessage.stickerSentTs = Date.now();
        if (cacheableKey) {
            logger?.debug({ cacheableKey }, 'set cache');
            await options.mediaCache.set(cacheableKey, proto.Message.encode(obj).finish());
        }
        return obj;
    }

    const requiresDuration = mediaType === 'audio' && typeof uploadData.seconds === 'undefined';
    const requiresThumb = (mediaType === 'image' || mediaType === 'video') && typeof uploadData.jpegThumbnail === 'undefined';
    const requiresWaveform = mediaType === 'audio' && uploadData.ptt === true && typeof uploadData.waveform === 'undefined';
    const requiresBg = !!(options.backgroundColor && mediaType === 'audio' && uploadData.ptt === true);
    const saveOriginal = requiresDuration || requiresThumb;

    const { mediaKey, encFilePath, originalFilePath, fileEncSha256, fileSha256, fileLength } = await encryptedStream(
        uploadData.media,
        options.mediaTypeOverride || mediaType,
        { logger, saveOriginalFileIfRequired: saveOriginal, opts: options.options }
    );

    const fileEncSha256B64 = fileEncSha256.toString('base64');
    const [{ mediaUrl, directPath }] = await Promise.all([
        options.upload(encFilePath, { fileEncSha256B64, mediaType, timeoutMs: options.mediaUploadTimeoutMs }),
        (async () => {
            try {
                if (requiresThumb) {
                    const { thumbnail, originalImageDimensions } = await generateThumbnail(originalFilePath, mediaType, options);
                    uploadData.jpegThumbnail = thumbnail;
                    if (!uploadData.width && originalImageDimensions) {
                        uploadData.width = originalImageDimensions.width;
                        uploadData.height = originalImageDimensions.height;
                    }
                }
                if (requiresDuration) uploadData.seconds = await getAudioDuration(originalFilePath);
                if (requiresWaveform) uploadData.waveform = await getAudioWaveform(originalFilePath, logger);
                if (requiresBg) uploadData.backgroundArgb = await assertColor(options.backgroundColor);
            } catch (err) {
                logger?.warn({ trace: err.stack }, 'failed to obtain extra media info');
            }
        })()
    ]).finally(async () => {
        await fs.unlink(encFilePath).catch(() => {});
        if (originalFilePath) await fs.unlink(originalFilePath).catch(() => {});
    });

    const obj = proto.Message.fromObject({
        [`${mediaType}Message`]: MessageTypeProto[mediaType].fromObject({
            url: mediaUrl, directPath, mediaKey, fileEncSha256, fileSha256, fileLength,
            mediaKeyTimestamp: unixTimestampSeconds(), ...uploadData, media: undefined
        })
    });
    if (uploadData.ptv) { obj.ptvMessage = obj.videoMessage; delete obj.videoMessage; }
    if (cacheableKey) {
        logger?.debug({ cacheableKey }, 'set cache');
        await options.mediaCache.set(cacheableKey, proto.Message.encode(obj).finish());
    }
    return obj;
};

export const prepareDisappearingMessageSettingContent = (ephemeralExpiration = 0) => proto.Message.fromObject({
    ephemeralMessage: {
        message: {
            protocolMessage: {
                type: proto.Message.ProtocolMessage.Type.EPHEMERAL_SETTING,
                ephemeralExpiration
            }
        }
    }
});

export const generateForwardMessageContent = (message, forceForward) => {
    let content = normalizeMessageContent(message.message);
    if (!content) throw new Boom('no content', { statusCode: 400 });
    content = proto.Message.decode(proto.Message.encode(content).finish());
    let key = Object.keys(content)[0];
    let score = (content[key]?.contextInfo?.forwardingScore || 0) + (message.key.fromMe && !forceForward ? 0 : 1);
    if (key === 'conversation') {
        content.extendedTextMessage = { text: content[key] };
        delete content.conversation;
        key = 'extendedTextMessage';
    }
    const target = content[key];
    target.contextInfo = { forwardingScore: score, isForwarded: true, ...(score === 0 ? {} : {}) };
    return content;
};

export const hasNonNullishProperty = (message, key) => message && typeof message === 'object' && key in message && message[key] != null;
const hasOptionalProperty = (obj, key) => obj && typeof obj === 'object' && key in obj && obj[key] != null;

export const generateWAMessageContent = async (message, options) => {
    let m = {};

    if (hasNonNullishProperty(message, 'text')) {
        const ext = { text: message.text };
        let urlInfo = message.linkPreview ?? await generateLinkPreviewIfRequired(message.text, options.getUrlInfo, options.logger);
        if (urlInfo) {
            ext.matchedText = urlInfo['matched-text'];
            ext.jpegThumbnail = urlInfo.jpegThumbnail;
            ext.description = urlInfo.description;
            ext.title = urlInfo.title;
            ext.previewType = 0;
            const img = urlInfo.highQualityThumbnail;
            if (img) {
                ext.thumbnailDirectPath = img.directPath;
                ext.mediaKey = img.mediaKey;
                ext.mediaKeyTimestamp = img.mediaKeyTimestamp;
                ext.thumbnailWidth = img.width;
                ext.thumbnailHeight = img.height;
                ext.thumbnailSha256 = img.fileSha256;
                ext.thumbnailEncSha256 = img.fileEncSha256;
            }
        }
        if (options.backgroundColor) ext.backgroundArgb = await assertColor(options.backgroundColor);
        if (options.font) ext.font = options.font;
        m.extendedTextMessage = ext;
    } else if (hasNonNullishProperty(message, 'contacts')) {
        const contacts = message.contacts.contacts;
        if (!contacts.length) throw new Boom('require at least 1 contact', { statusCode: 400 });
        if (contacts.length === 1) m.contactMessage = proto.Message.ContactMessage.create(contacts[0]);
        else m.contactsArrayMessage = proto.Message.ContactsArrayMessage.create(message.contacts);
    } else if (hasNonNullishProperty(message, 'location')) {
        m.locationMessage = proto.Message.LocationMessage.create(message.location);
    } else if (hasNonNullishProperty(message, 'react')) {
        if (!message.react.senderTimestampMs) message.react.senderTimestampMs = Date.now();
        m.reactionMessage = proto.Message.ReactionMessage.create(message.react);
    } else if (hasNonNullishProperty(message, 'delete')) {
        m.protocolMessage = { key: message.delete, type: proto.Message.ProtocolMessage.Type.REVOKE };
    } else if (hasNonNullishProperty(message, 'forward')) {
        m = generateForwardMessageContent(message.forward, message.force);
    } else if (hasNonNullishProperty(message, 'disappearingMessagesInChat')) {
        const exp = typeof message.disappearingMessagesInChat === 'boolean'
            ? (message.disappearingMessagesInChat ? WA_DEFAULT_EPHEMERAL : 0)
            : message.disappearingMessagesInChat;
        m = prepareDisappearingMessageSettingContent(exp);
    } else if (hasNonNullishProperty(message, 'groupInvite')) {
        const inv = message.groupInvite;
        m.groupInviteMessage = {
            inviteCode: inv.inviteCode, inviteExpiration: inv.inviteExpiration,
            caption: inv.text, groupJid: inv.jid, groupName: inv.subject
        };
        if (options.getProfilePicUrl) {
            const pfpUrl = await options.getProfilePicUrl(inv.jid, 'preview');
            if (pfpUrl) {
                const resp = await fetch(pfpUrl, { method: 'GET', dispatcher: options?.options?.dispatcher });
                if (resp.ok) m.groupInviteMessage.jpegThumbnail = Buffer.from(await resp.arrayBuffer());
            }
        }
    } else if (hasNonNullishProperty(message, 'pin')) {
        m.pinInChatMessage = { key: message.pin, type: message.type, senderTimestampMs: Date.now() };
        m.messageContextInfo = { messageAddOnDurationInSecs: message.type === 1 ? (message.time || 86400) : 0 };
    } else if (hasNonNullishProperty(message, 'buttonReply')) {
        if (message.type === 'template') {
            m.templateButtonReplyMessage = {
                selectedDisplayText: message.buttonReply.displayText,
                selectedId: message.buttonReply.id,
                selectedIndex: message.buttonReply.index
            };
        } else {
            m.buttonsResponseMessage = {
                selectedButtonId: message.buttonReply.id,
                selectedDisplayText: message.buttonReply.displayText,
                type: proto.Message.ButtonsResponseMessage.Type.DISPLAY_TEXT
            };
        }
    } else if (hasOptionalProperty(message, 'ptv') && message.ptv) {
        const { videoMessage } = await prepareWAMessageMedia({ video: message.video }, options);
        m.ptvMessage = videoMessage;
    } else if (hasNonNullishProperty(message, 'product')) {
        const { imageMessage } = await prepareWAMessageMedia({ image: message.product.productImage }, options);
        m.productMessage = proto.Message.ProductMessage.create({ ...message, product: { ...message.product, productImage: imageMessage } });
    } else if (hasNonNullishProperty(message, 'listReply')) {
        m.listResponseMessage = { ...message.listReply };
    } else if (hasNonNullishProperty(message, 'event')) {
        const ev = message.event;
        const startTime = Math.floor(ev.startDate.getTime() / 1000);
        m.eventMessage = {};
        if (ev.call && options.getCallLink) {
            const token = await options.getCallLink(ev.call, { startTime });
            m.eventMessage.joinLink = (ev.call === 'audio' ? CALL_AUDIO_PREFIX : CALL_VIDEO_PREFIX) + token;
        }
        m.messageContextInfo = { messageSecret: ev.messageSecret || randomBytes(32) };
        m.eventMessage.name = ev.name;
        m.eventMessage.description = ev.description;
        m.eventMessage.startTime = startTime;
        m.eventMessage.endTime = ev.endDate ? ev.endDate.getTime() / 1000 : undefined;
        m.eventMessage.isCanceled = ev.isCancelled ?? false;
        m.eventMessage.extraGuestsAllowed = ev.extraGuestsAllowed;
        m.eventMessage.isScheduleCall = ev.isScheduleCall ?? false;
        m.eventMessage.location = ev.location;
    } else if (hasNonNullishProperty(message, 'poll')) {
        const poll = message.poll;
        poll.selectableCount ??= 0;
        poll.toAnnouncementGroup ??= false;
        if (!Array.isArray(poll.values)) throw new Boom('Invalid poll values', { statusCode: 400 });
        if (poll.selectableCount < 0 || poll.selectableCount > poll.values.length) {
            throw new Boom(`poll.selectableCount must be 0..${poll.values.length}`, { statusCode: 400 });
        }
        m.messageContextInfo = { messageSecret: poll.messageSecret || randomBytes(32) };
        const pollMsg = { name: poll.name, selectableOptionsCount: poll.selectableCount, options: poll.values.map(v => ({ optionName: v })) };
        if (poll.toAnnouncementGroup) m.pollCreationMessageV2 = pollMsg;
        else if (poll.selectableCount === 1) m.pollCreationMessageV3 = pollMsg;
        else m.pollCreationMessage = pollMsg;
    } else if (hasNonNullishProperty(message, 'album')) {
        m.albumMessage = { expectedImageCount: message.album.expectedImageCount, expectedVideoCount: message.album.expectedVideoCount };
    } else if (hasNonNullishProperty(message, 'sharePhoneNumber')) {
        m.protocolMessage = { type: proto.Message.ProtocolMessage.Type.SHARE_PHONE_NUMBER };
    } else if (hasNonNullishProperty(message, 'requestPhoneNumber')) {
        m.requestPhoneNumberMessage = {};
    } else if (hasNonNullishProperty(message, 'limitSharing')) {
        m.protocolMessage = {
            type: proto.Message.ProtocolMessage.Type.LIMIT_SHARING,
            limitSharing: {
                sharingLimited: message.limitSharing === true,
                trigger: 1,
                limitSharingSettingTimestamp: Date.now(),
                initiatedByMe: true
            }
        };
    } else {
        m = await prepareWAMessageMedia(message, options);
    }

    // BUTTONS
    if (hasNonNullishProperty(message, 'buttons') && message.buttons) {
        const btnMsg = { buttons: message.buttons.map(b => ({ ...b, type: proto.Message.ButtonsMessage.Button.Type.RESPONSE })) };
        const hasMedia = !!(m.imageMessage || m.videoMessage || m.documentMessage || m.audioMessage);
        if (hasMedia) {
            if (m.imageMessage) { btnMsg.imageMessage = m.imageMessage; btnMsg.headerType = ButtonType.IMAGE; }
            else if (m.videoMessage) { btnMsg.videoMessage = m.videoMessage; btnMsg.headerType = ButtonType.VIDEO; }
            else if (m.documentMessage) { btnMsg.documentMessage = m.documentMessage; btnMsg.headerType = ButtonType.DOCUMENT; }
            if (hasNonNullishProperty(message, 'caption')) btnMsg.contentText = message.caption;
        } else if (hasNonNullishProperty(message, 'text')) {
            btnMsg.contentText = message.text;
            btnMsg.headerType = ButtonType.EMPTY;
        } else if (hasNonNullishProperty(message, 'caption')) {
            btnMsg.contentText = message.caption;
            btnMsg.headerType = ButtonType.EMPTY;
        }
        if (hasNonNullishProperty(message, 'title') && message.title) { btnMsg.text = message.title; btnMsg.headerType = ButtonType.TEXT; }
        if (hasNonNullishProperty(message, 'footer')) btnMsg.footerText = message.footer;
        if (hasNonNullishProperty(message, 'contextInfo')) btnMsg.contextInfo = message.contextInfo;
        if (hasNonNullishProperty(message, 'mentions')) { btnMsg.contextInfo = btnMsg.contextInfo || {}; btnMsg.contextInfo.mentionedJid = message.mentions; }
        m = { buttonsMessage: btnMsg };
    }
    // TEMPLATE BUTTONS
    else if (hasNonNullishProperty(message, 'templateButtons') && message.templateButtons) {
        const tpl = { hydratedButtons: message.templateButtons };
        if (hasNonNullishProperty(message, 'text')) tpl.hydratedContentText = message.text;
        else if (hasNonNullishProperty(message, 'caption')) { tpl.hydratedContentText = message.caption; Object.assign(tpl, m); }
        if (hasNonNullishProperty(message, 'footer')) tpl.hydratedFooterText = message.footer;
        m = { templateMessage: { fourRowTemplate: tpl, hydratedTemplate: tpl } };
    }
    // LIST MESSAGES
    else if (hasNonNullishProperty(message, 'sections') && message.sections) {
        m = {
            listMessage: {
                sections: message.sections,
                buttonText: message.buttonText,
                title: message.title,
                footerText: message.footer,
                description: message.text || message.caption,
                listType: proto.Message.ListMessage.ListType.SINGLE_SELECT
            }
        };
    }
    // INTERACTIVE BUTTONS
    else if (hasNonNullishProperty(message, 'interactiveButtons') && message.interactiveButtons) {
        const inter = { nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.fromObject({ buttons: message.interactiveButtons }) };
        if (hasNonNullishProperty(message, 'text')) inter.body = { text: message.text };
        else if (hasNonNullishProperty(message, 'caption')) {
            inter.body = { text: message.caption };
            if (hasNonNullishProperty(message, 'title') || hasNonNullishProperty(message, 'subtitle')) {
                inter.header = {
                    title: message.title, subtitle: message.subtitle,
                    hasMediaAttachment: !!(message.media || (Object.keys(m).length && (m.imageMessage || m.videoMessage)))
                };
                Object.assign(inter.header, m);
            }
        }
        if (hasNonNullishProperty(message, 'footer')) inter.footer = { text: message.footer };
        if (hasNonNullishProperty(message, 'title')) {
            inter.header = {
                title: message.title, subtitle: message.subtitle,
                hasMediaAttachment: !!(message.media || (Object.keys(m).length && (m.imageMessage || m.videoMessage)))
            };
            Object.assign(inter.header, m);
        }
        if (hasNonNullishProperty(message, 'contextInfo')) inter.contextInfo = message.contextInfo;
        if (hasNonNullishProperty(message, 'mentions')) { inter.contextInfo = inter.contextInfo || {}; inter.contextInfo.mentionedJid = message.mentions; }
        m = { interactiveMessage: inter };
    }

    if (hasOptionalProperty(message, 'viewOnce') && message.viewOnce) m = { viewOnceMessage: { message: m } };
    if ((hasOptionalProperty(message, 'mentions') && message.mentions?.length) || (hasOptionalProperty(message, 'mentionAll') && message.mentionAll)) {
        const msgType = Object.keys(m)[0];
        const target = m[msgType];
        if (target && 'contextInfo' in target) {
            target.contextInfo = target.contextInfo || {};
            if (message.mentions?.length) target.contextInfo.mentionedJid = message.mentions;
            if (message.mentionAll) target.contextInfo.nonJidMentions = 1;
        } else if (target) {
            target.contextInfo = { mentionedJid: message.mentions, nonJidMentions: message.mentionAll ? 1 : 0 };
        }
    }
    if (hasOptionalProperty(message, 'edit')) {
        m = { protocolMessage: { key: message.edit, editedMessage: m, timestampMs: Date.now(), type: proto.Message.ProtocolMessage.Type.MESSAGE_EDIT } };
    }
    if (hasOptionalProperty(message, 'contextInfo') && message.contextInfo) {
        const msgType = Object.keys(m)[0];
        const target = m[msgType];
        if (target && 'contextInfo' in target && target.contextInfo) target.contextInfo = { ...target.contextInfo, ...message.contextInfo };
        else if (target) target.contextInfo = message.contextInfo;
    }
    if (hasOptionalProperty(message, 'albumParentKey') && message.albumParentKey) {
        m.messageContextInfo = {
            ...m.messageContextInfo,
            messageAssociation: { associationType: proto.MessageAssociation.AssociationType.MEDIA_ALBUM, parentMessageKey: message.albumParentKey }
        };
    }
    if (shouldIncludeReportingToken(m)) {
        m.messageContextInfo = m.messageContextInfo || {};
        if (!m.messageContextInfo.messageSecret) m.messageContextInfo.messageSecret = randomBytes(32);
    }
    return proto.Message.create(m);
};

export const generateWAMessageFromContent = (jid, message, options) => {
    if (!options.timestamp) options.timestamp = new Date();
    const inner = normalizeMessageContent(message);
    const key = getContentType(inner);
    const timestamp = unixTimestampSeconds(options.timestamp);
    const { quoted, userJid } = options;
    if (quoted && !isJidNewsletter(jid)) {
        const participant = quoted.key.fromMe ? userJid : (quoted.participant || quoted.key.participant || quoted.key.remoteJid);
        let quotedMsg = normalizeMessageContent(quoted.message);
        const qType = getContentType(quotedMsg);
        quotedMsg = proto.Message.create({ [qType]: quotedMsg[qType] });
        const quotedContent = quotedMsg[qType];
        if (quotedContent && typeof quotedContent === 'object' && 'contextInfo' in quotedContent) delete quotedContent.contextInfo;
        const ctx = ('contextInfo' in inner[key] && inner[key]?.contextInfo) || {};
        ctx.participant = jidNormalizedUser(participant);
        ctx.stanzaId = quoted.key.id;
        ctx.quotedMessage = quotedMsg;
        if (jid !== quoted.key.remoteJid) ctx.remoteJid = quoted.key.remoteJid;
        if (ctx && inner[key]) inner[key].contextInfo = ctx;
    }
    if (options?.ephemeralExpiration && key !== 'protocolMessage' && key !== 'ephemeralMessage' && !isJidNewsletter(jid)) {
        inner[key].contextInfo = { ...(inner[key].contextInfo || {}), expiration: options.ephemeralExpiration || WA_DEFAULT_EPHEMERAL };
    }
    message = proto.Message.create(message);
    return proto.WebMessageInfo.fromObject({
        key: { remoteJid: jid, fromMe: true, id: options?.messageId || generateMessageIDV2() },
        message,
        messageTimestamp: timestamp,
        messageStubParameters: [],
        participant: (isJidGroup(jid) || isJidStatusBroadcast(jid)) ? userJid : undefined,
        status: WAMessageStatus.PENDING
    });
};

export const generateWAMessage = async (jid, content, options) => {
    options.logger = options?.logger?.child({ msgId: options.messageId });
    return generateWAMessageFromContent(jid, await generateWAMessageContent(content, { ...options, jid }), options);
};

export const getContentType = (content) => {
    if (!content) return;
    return Object.keys(content).find(k => (k === 'conversation' || k.includes('Message')) && k !== 'senderKeyDistributionMessage');
};

export const normalizeMessageContent = (content) => {
    if (!content) return;
    for (let i = 0; i < 5; i++) {
        const inner = content?.ephemeralMessage || content?.viewOnceMessage || content?.documentWithCaptionMessage ||
                      content?.viewOnceMessageV2 || content?.viewOnceMessageV2Extension || content?.editedMessage ||
                      content?.associatedChildMessage || content?.groupStatusMessage || content?.groupStatusMessageV2;
        if (!inner) break;
        content = inner.message;
    }
    return content;
};

export const extractMessageContent = (content) => {
    const extract = (msg) => {
        if (msg.imageMessage) return { imageMessage: msg.imageMessage };
        if (msg.documentMessage) return { documentMessage: msg.documentMessage };
        if (msg.videoMessage) return { videoMessage: msg.videoMessage };
        if (msg.locationMessage) return { locationMessage: msg.locationMessage };
        return { conversation: msg.contentText || msg.hydratedContentText || '' };
    };
    content = normalizeMessageContent(content);
    if (content?.buttonsMessage) return extract(content.buttonsMessage);
    if (content?.templateMessage?.hydratedFourRowTemplate) return extract(content.templateMessage.hydratedFourRowTemplate);
    if (content?.templateMessage?.hydratedTemplate) return extract(content.templateMessage.hydratedTemplate);
    if (content?.templateMessage?.fourRowTemplate) return extract(content.templateMessage.fourRowTemplate);
    return content;
};

export const getDevice = (id) => /^3A.{18}$/.test(id) ? 'ios' : /^3E.{20}$/.test(id) ? 'web' : /^(.{21}|.{32})$/.test(id) ? 'android' : /^(3F|.{18}$)/.test(id) ? 'desktop' : 'unknown';

export const updateMessageWithReceipt = (msg, receipt) => {
    msg.userReceipt = msg.userReceipt || [];
    const existing = msg.userReceipt.find(r => r.userJid === receipt.userJid);
    if (existing) Object.assign(existing, receipt);
    else msg.userReceipt.push(receipt);
};

export const updateMessageWithReaction = (msg, reaction) => {
    const author = getKeyAuthor(reaction.key);
    msg.reactions = (msg.reactions || []).filter(r => getKeyAuthor(r.key) !== author);
    reaction.text = reaction.text || '';
    msg.reactions.push(reaction);
};

export const updateMessageWithPollUpdate = (msg, update) => {
    const author = getKeyAuthor(update.pollUpdateMessageKey);
    msg.pollUpdates = (msg.pollUpdates || []).filter(u => getKeyAuthor(u.pollUpdateMessageKey) !== author);
    if (update.vote?.selectedOptions?.length) msg.pollUpdates.push(update);
};

export const updateMessageWithEventResponse = (msg, update) => {
    const author = getKeyAuthor(update.eventResponseMessageKey);
    msg.eventResponses = (msg.eventResponses || []).filter(r => getKeyAuthor(r.eventResponseMessageKey) !== author);
    msg.eventResponses.push(update);
};

export function getAggregateVotesInPollMessage({ message, pollUpdates }, meId) {
    const opts = message?.pollCreationMessage?.options || message?.pollCreationMessageV2?.options || message?.pollCreationMessageV3?.options || [];
    const map = {};
    for (const opt of opts) {
        const hash = sha256(Buffer.from(opt.optionName || '')).toString();
        map[hash] = { name: opt.optionName || '', voters: [] };
    }
    for (const upd of pollUpdates || []) {
        if (!upd.vote) continue;
        for (const optHash of upd.vote.selectedOptions || []) {
            const h = optHash.toString();
            if (!map[h]) map[h] = { name: 'Unknown', voters: [] };
            map[h].voters.push(getKeyAuthor(upd.pollUpdateMessageKey, meId));
        }
    }
    return Object.values(map);
}

export function getAggregateResponsesInEventMessage({ eventResponses }, meId) {
    const types = ['GOING', 'NOT_GOING', 'MAYBE'];
    const map = {};
    for (const t of types) map[t] = { response: t, responders: [] };
    for (const upd of eventResponses || []) {
        const resp = upd.eventResponse || 'UNKNOWN';
        if (resp !== 'UNKNOWN' && map[resp]) map[resp].responders.push(getKeyAuthor(upd.eventResponseMessageKey, meId));
    }
    return Object.values(map);
}

export const aggregateMessageKeysNotFromMe = (keys) => {
    const groups = {};
    for (const { remoteJid, id, participant, fromMe } of keys) {
        if (fromMe) continue;
        const key = `${remoteJid}:${participant || ''}`;
        if (!groups[key]) groups[key] = { jid: remoteJid, participant, messageIds: [] };
        groups[key].messageIds.push(id);
    }
    return Object.values(groups);
};

const REUPLOAD_REQUIRED_STATUS = [410, 404];

export const downloadMediaMessage = async (message, type, options, ctx) => {
    const downloadMsg = async () => {
        const mContent = extractMessageContent(message.message);
        if (!mContent) throw new Boom('No message present', { statusCode: 400, data: message });
        const contentType = getContentType(mContent);
        let mediaType = contentType?.replace('Message', '');
        const media = mContent[contentType];
        if (!media || typeof media !== 'object' || (!('url' in media) && !('thumbnailDirectPath' in media))) {
            throw new Boom(`"${contentType}" not a media message`);
        }
        let download;
        if ('thumbnailDirectPath' in media && !('url' in media)) {
            download = { directPath: media.thumbnailDirectPath, mediaKey: media.mediaKey };
            mediaType = 'thumbnail-link';
        } else {
            download = media;
        }
        const stream = await downloadContentFromMessage(download, mediaType, options);
        if (type === 'buffer') {
            const chunks = [];
            for await (const chunk of stream) chunks.push(chunk);
            return Buffer.concat(chunks);
        }
        return stream;
    };
    return downloadMsg().catch(async (err) => {
        if (ctx && typeof err?.status === 'number' && REUPLOAD_REQUIRED_STATUS.includes(err.status)) {
            ctx.logger.info({ key: message.key }, 'requesting media reupload');
            message = await ctx.reuploadRequest(message);
            return downloadMsg();
        }
        throw err;
    });
};

export const assertMediaContent = (content) => {
    content = extractMessageContent(content);
    const media = content?.documentMessage || content?.imageMessage || content?.videoMessage || content?.audioMessage || content?.stickerMessage;
    if (!media) throw new Boom('Not a media message', { statusCode: 400, data: content });
    return media;
};
