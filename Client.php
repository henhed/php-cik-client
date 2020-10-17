<?php
/**
 * See COPYING.txt for license details.
 *
 * @copyright Copyright (c) Henhed AB.
 */
declare(strict_types=1);

namespace Henhed\CiK;

use Henhed\CiK\Exception\NetworkException;
use Henhed\CiK\Exception\ProtocolException;
use Henhed\CiK\Exception\ServerException;
use Henhed\CiK\Exception\ClientException;
use Henhed\CiK\Exception\MessageException;

class Client
{

    /**#@+
     * Protocol control byte values
     */
    const CONTROL_BYTE_1 = 0x43; // 'C'
    const CONTROL_BYTE_2 = 0x69; // 'i'
    const CONTROL_BYTE_3 = 0x4B; // 'K'
    const CMD_BYTE_GET   = 0x67; // 'g'
    const CMD_BYTE_SET   = 0x73; // 's'
    const CMD_BYTE_DEL   = 0x64; // 'd'
    const CMD_BYTE_CLR   = 0x63; // 'c'
    const CMD_BYTE_LST   = 0x6C; // 'l'
    const CMD_BYTE_NFO   = 0x6E; // 'n'
    /**#@-*/

    /**#@+
     * Response status indicators
     */
    const SUCCESS_BYTE   = 0x74; // 't'
    const FAILURE_BYTE   = 0x66; // 'f'
    /**#@-*/

    /**#@+
     * Flags
     */
    const FLAG_GET_NONE           = 0x00;
    const FLAG_GET_IGNORE_EXPIRES = 0x01;
    const FLAG_SET_NONE           = 0x00;
    const FLAG_SET_ONLY_TTL       = 0x01;
    /**#@-*/

    /**#@+
     * Clear modes
     */
    const CLEAR_MODE_ALL        = 0x00;
    const CLEAR_MODE_OLD        = 0x01;
    const CLEAR_MODE_MATCH_ALL  = 0x02;
    const CLEAR_MODE_MATCH_NONE = 0x03;
    const CLEAR_MODE_MATCH_ANY  = 0x04;
    /**#@-*/

    /**#@+
     * List modes
     */
    const LIST_MODE_ALL_KEYS   = 0x00;
    const LIST_MODE_ALL_TAGS   = 0x01;
    const LIST_MODE_MATCH_ALL  = 0x02;
    const LIST_MODE_MATCH_NONE = 0x03;
    const LIST_MODE_MATCH_ANY  = 0x04;
    /**#@-*/

    /**#@+
     * Status codes
     */
    const STATUS_OK                     = 0x00;
    const MASK_INTERNAL_ERROR           = 0x10;
    const STATUS_BUG                    = 0x11;
    const STATUS_CONNECTION_CLOSED      = 0x12;
    const STATUS_NETWORK_ERROR          = 0x13;
    const MASK_CLIENT_ERROR             = 0x20;
    const STATUS_PROTOCOL_ERROR         = 0x21;
    const MASK_CLIENT_MESSAGE           = 0x40;
    const STATUS_NOT_FOUND              = 0x41;
    const STATUS_EXPIRED                = 0x42;
    const STATUS_OUT_OF_MEMORY          = 0x43;
    const STATUS_LIMIT_EXCEEDED         = 0x44;
    /**#@-*/

    /** @var string */
    private $address = null;

    /** @var resource */
    private $fd = false;

    /**
     * Constructor
     *
     * @param string $host
     * @param int    $port
     */
    public function __construct(string $address = 'tcp://127.0.0.1:20274')
    {
        $this->address = $address;
    }

    /**
     * Destructor
     */
    public function __destruct()
    {
        $this->disconnect();
    }

    /**
     * @api
     *
     * @param  string $key
     * @param  int    $flags
     * @return string
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     * @throws MessageException
     */
    public function get(string $key, int $flags = self::FLAG_GET_NONE): string
    {
        $this->writeMessage($this->makeGetMessage($key, $flags));
        return $this->readResponse();
    }

    /**
     * @api
     *
     * @param  string $key
     * @param  string $value
     * @param  array  $tags
     * @param  int    $ttl
     * @param  int    $flags
     * @return bool
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     */
    public function set(
        string $key,
        string $value,
        array  $tags  = [],
        int    $ttl   = -1,
        int    $flags = self::FLAG_SET_NONE
    ): bool {
        $message = $this->makeSetMessage($key, $value, $tags, $ttl, $flags);
        try {
            $this->writeMessage($message);
            return ('' === $this->readResponse());
        } catch (MessageException $e) {
            return false;
        }
    }

    /**
     * @api
     *
     * @param  string $key
     * @return bool
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     */
    public function del(string $key): bool
    {
        $message = $this->makeDelMessage($key);
        try {
            $this->writeMessage($message);
            return ('' === $this->readResponse());
        } catch (MessageException $e) {
            return false;
        }
    }

    /**
     * @api
     *
     * @param  int      $mode
     * @param  string[] $tags
     * @return bool
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     */
    public function clr(int $mode = self::CLEAR_MODE_ALL, array $tags = []): bool
    {
        static $modes = [
            self::CLEAR_MODE_ALL        => 'ALL',
            self::CLEAR_MODE_OLD        => 'OLD',
            self::CLEAR_MODE_MATCH_ALL  => 'MATCH ALL',
            self::CLEAR_MODE_MATCH_NONE => 'MATCH NONE',
            self::CLEAR_MODE_MATCH_ANY  => 'MATCH ANY',
        ];
        if (!isset($modes[$mode])) {
            throw new ProtocolException(\sprintf(
                'Invalid clear mode 0x%X',
                $mode
            ));
        }

        if ((($mode == self::CLEAR_MODE_ALL)
             || ($mode == self::CLEAR_MODE_OLD))
            && !empty($tags)
        ) {
            throw new ProtocolException(\sprintf(
                'Clear mode %s must not include tags',
                $modes[$mode]
            ));
        }

        $message = $this->makeClrMessage($mode, $tags);
        try {
            $this->writeMessage($message);
            return ('' === $this->readResponse());
        } catch (MessageException $e) {
            return false;
        }
    }

    /**
     * @api
     *
     * @param  int      $mode
     * @param  string[] $tags
     * @return string[]
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     * @throws MessageException
     */
    public function lst(
        int   $mode = self::LIST_MODE_ALL_KEYS,
        array $tags = []
    ): array {
        $tags = $this->formatTags($tags);
        $head = \pack(
            'c3cCC@16',
            self::CONTROL_BYTE_1,
            self::CONTROL_BYTE_2,
            self::CONTROL_BYTE_3,
            self::CMD_BYTE_LST,
            $mode,
            \count($tags)
        );
        $message = $head . $this->makeTagsMessage($tags);
        $this->writeMessage($message);
        $response = $this->readResponse();

        $keys = [];

        while (\strlen((string) $response) > 0) {
            $klen     = \ord($response);
            $keys[]   = \substr($response, 1, $klen);
            $response = \substr($response, 1 + $klen);
        }

        return $keys;
    }

    /**
     * @api
     *
     * @param  string|null $key
     * @return array
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     * @throws MessageException
     */
    public function nfo(?string $key = null): array
    {
        $srv  = ($key === null);
        $key  = $this->formatKey((string) $key);
        $klen = \strlen($key);
        $head = \pack(
            'c3cC@16',
            self::CONTROL_BYTE_1,
            self::CONTROL_BYTE_2,
            self::CONTROL_BYTE_3,
            self::CMD_BYTE_NFO,
            $klen
        );
        $message = $head . $key;
        $this->writeMessage($message);
        $response = $this->readResponse();

        if ($srv) {
            $nfo = @\unpack('Jmemory_used/Jmemory_free', $response);
            if (!\is_array($nfo)) {
                throw new ProtocolException(\sprintf(
                    'Failed to parse CiK NFO response: 0x%s',
                    \strtoupper(\bin2hex($response))
                ));
            }
            return $nfo;
        } else {
            $nfo = @\unpack('Jexpires/Jmtime', $response);
            if (!\is_array($nfo)) {
                throw new ProtocolException(\sprintf(
                    'Failed to parse CiK NFO response: 0x%s',
                    \strtoupper(\bin2hex($response))
                ));
            }
            if ($nfo['expires'] === -1) {
                $nfo['expires'] = false;
            }
            $nfo['tags'] = [];
            $response = \substr($response, 16);
            while (\strlen((string) $response) > 0) {
                $tlen = \ord($response);
                if ($tlen === 0) {
                    throw new ProtocolException('Got zero length tag from server');
                }
                $nfo['tags'][] = \substr($response, 1, $tlen);
                $response = \substr($response, 1 + $tlen);
            }
            return $nfo;
        }
    }

    /**
     * @param  string[] $tags
     * @return string
     */
    private function makeTagsMessage(array $tags): string
    {
        $callback = function (string $tag): string {
            return \pack('C', \strlen($tag)) . $tag;
        };
        $messages = \array_map($callback, $tags);
        return \implode('', $messages);
    }

    /**
     * @param  string $key
     * @param  int    $flags
     * @return string
     */
    private function makeGetMessage(
        string $key,
        int    $flags = self::FLAGS_GET_NONE
    ): string {
        $key  = $this->formatKey($key);
        $klen = \strlen($key);
        $head = \pack(
            'c3cCC@16',
            self::CONTROL_BYTE_1,
            self::CONTROL_BYTE_2,
            self::CONTROL_BYTE_3,
            self::CMD_BYTE_GET,
            $klen,
            $flags
        );
        return $head . $key;
    }

    /**
     * @param  string $key
     * @param  string $value
     * @param  array  $tags
     * @param  int    $ttl
     * @param  int    $flags
     * @return string
     */
    private function makeSetMessage(
        string $key,
        string $value,
        array  $tags,
        int    $ttl,
        int    $flags = self::FLAGS_SET_NONE
    ): string {
        $key  = $this->formatKey($key);
        $tags = $this->formatTags($tags);
        $klen = \strlen($key);
        $tlen = \count($tags);
        $vlen = \strlen($value);
        $head = \pack(
            'c3cCCC@8NN',
            self::CONTROL_BYTE_1,
            self::CONTROL_BYTE_2,
            self::CONTROL_BYTE_3,
            self::CMD_BYTE_SET,
            $klen,
            $tlen,
            $flags,
            $vlen,
            ($ttl >= 0) ? $ttl : 0xFFFFFFFF
        );
        return $head . $key . $this->makeTagsMessage($tags) . $value;
    }

    /**
     * @param  string $key
     * @return string
     */
    private function makeDelMessage(string $key): string
    {
        $key  = $this->formatKey($key);
        $klen = \strlen($key);
        $head = \pack(
            'c3cCC@16',
            self::CONTROL_BYTE_1,
            self::CONTROL_BYTE_2,
            self::CONTROL_BYTE_3,
            self::CMD_BYTE_DEL,
            $klen,
            0 // flags only for server internals
        );
        return $head . $key;
    }

    /**
     * @param  int      $mode
     * @param  string[] $tags
     * @return string
     */
    private function makeClrMessage(int $mode, array $tags = []): string
    {
        $tags = $this->formatTags($tags);
        $head = \pack(
            'c3cCCC@16',
            self::CONTROL_BYTE_1,
            self::CONTROL_BYTE_2,
            self::CONTROL_BYTE_3,
            self::CMD_BYTE_CLR,
            $mode,
            \count($tags),
            0 // flags only for server internals
        );
        return $head . $this->makeTagsMessage($tags);
    }

    /**
     * @param  string $key
     * @return string
     */
    private function formatKey(string $key): string
    {
        if (\strlen($key) > 0xFF) {
            return \hash('sha512', $key, false);
        }
        return $key;
    }

    /**
     * @param  string $tag
     * @return string
     */
    private function formatTag(string $tag): string
    {
        return $this->formatKey($tag);
    }

    /**
     * @param  string[] $tags
     * @return string[]
     */
    private function formatTags(array $tags): array
    {
        $tags = \array_filter($tags);
        $tags = \array_unique($tags);
        $tags = \array_slice($tags, 0, 0xFF);
        return \array_map([$this, 'formatTag'], $tags);
    }

    /**
     * @return string
     * @throws NetworkException
     * @throws ProtocolException
     * @throws ServerException
     * @throws ClientException
     * @throws MessageException
     */
    private function readResponse(): string
    {
        $header   = $this->readMessage(8);
        $response = @\unpack('c3cik/c1status/NsizeOrError', $header);
        if (!\is_array($response)) {
            $this->disconnect();
            throw new ProtocolException(\sprintf(
                'Failed to parse CiK response header: 0x%s',
                \strtoupper(\bin2hex($header))
            ));
        }
        if (($response['cik1'] !== self::CONTROL_BYTE_1)
            || ($response['cik2'] !== self::CONTROL_BYTE_2)
            || ($response['cik3'] !== self::CONTROL_BYTE_3)
            || (($response['status'] !== self::SUCCESS_BYTE)
                && ($response['status'] !== self::FAILURE_BYTE))
        ) {
            $this->disconnect();
            throw new ProtocolException(\sprintf(
                'Failed to parse CiK response header: 0x%s',
                \strtoupper(\bin2hex($header))
            ));
        }
        $success = ($response['status'] === self::SUCCESS_BYTE);
        if (!$success) {
            $errorCode = $response['sizeOrError'];
            $errorMessage = \sprintf('CiK returned error code 0x%X', $errorCode);
            if ($errorCode & self::MASK_INTERNAL_ERROR) {
                $this->disconnect();
                throw new ServerException($errorMessage, $errorCode);
            } elseif ($errorCode & self::MASK_CLIENT_ERROR) {
                $this->disconnect();
                throw new ClientException($errorMessage, $errorCode);
            } elseif ($errorCode & self::MASK_CLIENT_MESSAGE) {
                // @note: No disconnect here, not an error
                throw new MessageException($errorMessage, $errorCode);
            }
        }
        return $this->readMessage($response['sizeOrError']);
    }

    /**
     * @param  string $message
     * @return void
     * @throws NetworkException
     */
    private function writeMessage(string $message): void
    {
        $this->connect();
        $remainingSize = \strlen($message);
        while ($remainingSize > 0) {
            $nsent = @\fwrite($this->fd, $message);
            if (($nsent === false) || ($nsent === 0)) {
                $this->disconnect();
                $error = \error_get_last();
                throw new NetworkException(
                    $error['message'] ?? 'Connection Closed',
                    $error['type']    ?? 0
                );
            }
            $message = \substr($message, $nsent);
            $remainingSize -= $nsent;
        }
    }

    /**
     * @param  int $length
     * @return string
     * @throws NetworkException
     */
    private function readMessage(int $length): string
    {
        $this->connect();
        $message = '';
        while ($length > 0) {
            $chunk = @\fread($this->fd, $length);
            if (($chunk === false) || ($chunk === '')) {
                $this->disconnect();
                $error = \error_get_last();
                throw new NetworkException(
                    $error['message'] ?? 'Connection Closed',
                    $error['type']    ?? 0
                );
            }
            $message .= $chunk;
            $length -= \strlen($chunk);
        }
        return $message;
    }

    /**
     * Connect to CiK server if not already connected
     *
     * @return void
     * @throws NetworkException
     */
    private function connect(): void
    {
        if ($this->fd !== false) {
            return;
        }
        $errno = 0;
        $errstr = '';
        $timeout = 2.5;
        $flags = STREAM_CLIENT_CONNECT;
        $context = \stream_context_create(['socket' => ['tcp_nodelay' => true]]);
        $this->fd = @\stream_socket_client(
            $this->address,
            $errno,
            $errstr,
            $timeout,
            $flags,
            $context
        );
        if (!$this->fd) {
            throw new NetworkException('Could not connect: ' . $errstr, $errno);
        }
    }

    /**
     * Disconnect from CiK server if currently connected
     *
     * @return void
     */
    private function disconnect(): void
    {
        if ($this->fd !== false) {
            @\fclose($this->fd);
            $this->fd = false;
        }
    }
}
