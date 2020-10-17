<?php
/**
 * See COPYING.txt for license details.
 *
 * @copyright Copyright (c) Henhed AB.
 */
declare(strict_types=1);

namespace Henhed\CiK\Adapter;

use Henhed\CiK\Client;
use Henhed\CiK\Client\Exception\MessageException;

/**
 * Zend Framework cache backend adapter for CiK
 */
class ZF1 extends \Zend_Cache_Backend implements \Zend_Cache_Backend_ExtendedInterface
{

    /** @var array */
    protected $_options = [
        'server_address' => 'tcp://127.0.0.1:20274'
    ];

    /** @var Client */
    protected $_client = null;

    /**
     * @inheritDoc
     */
    public function __construct($options = [])
    {
        parent::__construct($options);
        $this->_client = new Client($this->getOption('server_address'));
    }

    /**
     * @inheritDoc
     */
    public function load($id, $doNotTestCacheValidity = false)
    {
        $flags = Client::FLAG_GET_NONE;
        if ($doNotTestCacheValidity) {
            $flags |= Client::FLAG_GET_IGNORE_EXPIRES;
        }

        try {
            return $this->_client->get((string) $id, $flags);
        } catch (\Exception $e) {
            if (!($e instanceof MessageException)) {
                $this->_log($e->getMessage());
            }
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function test($id)
    {
        return ($this->load($id) !== false);
    }

    /**
     * @inheritDoc
     */
    public function save($data, $id, $tags = [], $specificLifetime = false)
    {
        $ttl = $specificLifetime;
        if (($ttl === false) || ($ttl === null)) {
            $ttl = $this->getOption('lifetime');
        }

        try {
            return $this->_client->set(
                (string) $id,
                (string) $data,
                (array)  $tags,
                (int)    $ttl
            );
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function remove($id)
    {
        try {
            return $this->_client->del((string) $id);
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return false;
    }

    /**
     * @inheritDoc
     * @param  string $mode Clean mode
     * @param  array  $tags Array of tags
     * @return boolean true if no problem
     */
    public function clean($mode = \Zend_Cache::CLEANING_MODE_ALL, $tags = [])
    {
        switch ($mode) {
            case \Zend_Cache::CLEANING_MODE_ALL:
                $mode = Client::CLEAR_MODE_ALL;
                break;
            case \Zend_Cache::CLEANING_MODE_OLD:
                $mode = Client::CLEAR_MODE_OLD;
                break;
            case \Zend_Cache::CLEANING_MODE_MATCHING_TAG:
                $mode = Client::CLEAR_MODE_MATCH_ALL;
                break;
            case \Zend_Cache::CLEANING_MODE_NOT_MATCHING_TAG:
                $mode = Client::CLEAR_MODE_MATCH_NONE;
                break;
            case \Zend_Cache::CLEANING_MODE_MATCHING_ANY_TAG:
                $mode = Client::CLEAR_MODE_MATCH_ANY;
                break;
            default:
                \Zend_Cache::throwException(\sprintf(
                    'Unsupported clean mode: %s',
                    $mode
                ));
                break;
        }

        try {
            return $this->_client->clr((int) $mode, (array) $tags);
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function getIds()
    {
        try {
            return $this->_client->lst(Client::LIST_MODE_ALL_KEYS);
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return [];
    }

    /**
     * @inheritDoc
     */
    public function getTags()
    {
        try {
            return $this->_client->lst(Client::LIST_MODE_ALL_TAGS);
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return [];
    }

    /**
     * @inheritDoc
     */
    public function getIdsMatchingTags($tags = [])
    {
        try {
            return $this->_client->lst(
                Client::LIST_MODE_MATCH_ALL,
                (array) $tags
            );
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return [];
    }

    /**
     * @inheritDoc
     */
    public function getIdsNotMatchingTags($tags = [])
    {
        try {
            return $this->_client->lst(
                Client::LIST_MODE_MATCH_NONE,
                (array) $tags
            );
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return [];
    }

    /**
     * @inheritDoc
     */
    public function getIdsMatchingAnyTags($tags = [])
    {
        try {
            return $this->_client->lst(
                Client::LIST_MODE_MATCH_ANY,
                (array) $tags
            );
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return [];
    }

    /**
     * @inheritDoc
     */
    public function getFillingPercentage()
    {
        try {
            $info = $this->_client->nfo();
            $used = (float) ($info['memory_used'] ?? 0.0);
            $free = (float) ($info['memory_free'] ?? 0.0);
            $total = $used + $free;
            if ($total > 0.0) {
                return (int) \ceil(($used / $total) * 100.0);
            }
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }
        return 0;
    }

    /**
     * @inheritDoc
     */
    public function getMetadatas($id)
    {
        try {
            $info = $this->_client->nfo((string) $id);
            return [
                'expire' => $info['expires'] ?? 0,
                'tags'   => $info['tags']    ?? [],
                'mtime'  => $info['mtime']   ?? 0
            ];
        } catch (\Exception $e) {
            if (!($e instanceof MessageException)) {
                $this->_log($e->getMessage());
            }
        }
        return false;
    }

    /**
     * @inheritDoc
     */
    public function touch($id, $extraLifetime)
    {
        // Should $extraLifetime be relative to the previously set ttl? Unclear.
        try {
            return $this->_client->set(
                (string) $id,
                '',
                [],
                (int) $extraLifetime,
                Client::FLAG_SET_ONLY_TTL
            );
        } catch (\Exception $e) {
            $this->_log($e->getMessage());
        }

        return false;
    }

    /**
     * @inheritDoc
     */
    public function getCapabilities()
    {
        return [
            'automatic_cleaning'    => true,
            'tags'                  => true,
            'expired_read'          => true,
            'priority'              => false,
            'infinite_lifetime'     => true,
            'get_list'              => true
        ];
    }
}
