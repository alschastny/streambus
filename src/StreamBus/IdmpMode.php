<?php

declare(strict_types=1);

namespace StreamBus\StreamBus;

enum IdmpMode: string
{
    case None     = 'none';     // no idempotency (default)
    case Auto     = 'auto';     // IDMPAUTO — Redis deduplicates by content hash
    case Explicit = 'explicit'; // IDMP — caller supplies idempotentId per message
}
