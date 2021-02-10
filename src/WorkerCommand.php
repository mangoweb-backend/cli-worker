<?php declare(strict_types = 1);

namespace Mangoweb\CliWorker;

use Mangoweb\Clock\Clock;
use Psr\Log\LoggerInterface;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Input\InputOption;
use Symfony\Component\Console\Output\OutputInterface;


abstract class WorkerCommand extends Command
{
	/** exit code returned when command exits due to the risk of exhausting available memory */
	public const EXIT_CODE_MEMORY_LIMIT = 100;

	/** maximum percent of memory limit that can be safely consumed */
	private const MEMORY_LIMIT_MAX_CONSUMPTION = 0.8;

	/** @var LoggerInterface */
	protected $logger;

	/** @var int */
	protected $defaultLimit = 0;

	/** @var int */
	protected $defaultSleepTime = 15;

	/** @var int|null */
	protected $exitCode;


	public function __construct(LoggerInterface $logger)
	{
		parent::__construct();
		$this->logger = $logger;
	}


	protected function configure(): void
	{
		parent::configure();
		$this->addOption('limit', 'l', InputOption::VALUE_REQUIRED, '', $this->defaultLimit);
		$this->addOption('worker', 'w', InputOption::VALUE_NONE);
		$this->addOption('sleep', 's', InputOption::VALUE_REQUIRED, '', $this->defaultSleepTime);
	}


	protected function execute(InputInterface $input, OutputInterface $output): int
	{
		$count = 0;
		$limit = (int) $input->getOption('limit');
		$worker = (bool) $input->getOption('worker');
		$sleep = (int) $input->getOption('sleep');

		// SIGINT (which is invoked by CTRL+C) and SIGTERM handlers are replaced by custom handler
		// now both signals are not dispatched until pcntl_signal_dispatch() is called
		// which prevents termination of worker in the middle of a job
		if (function_exists('pcntl_signal')) {
			$callback = \Closure::fromCallable([$this, 'handleSignal']);
			pcntl_signal(SIGHUP, $callback);
			pcntl_signal(SIGINT, $callback);
			pcntl_signal(SIGTERM, $callback);
		}

		while ($limit === 0 || $count < $limit) {
			if (function_exists('pcntl_signal_dispatch')) {
				pcntl_signal_dispatch();
			}

			if ($this->riskOfNotEnoughMemory()) {
				$this->exitCode = $worker ? 0 : self::EXIT_CODE_MEMORY_LIMIT;
			}

			if ($this->exitCode !== null) {
				break;
			}

			Clock::refresh();

			try {
				$singleJobResult = $this->processSingleJob($input);

			} catch (\Throwable $e) {
				$this->logger->warning(sprintf('%s: unhandled exception: %s: %s; terminating', $this->getName(), get_class($e), $e->getMessage()), [
					'processedCount' => $count,
					'limit' => $limit,
				]);
				throw $e;
			}

			if ($singleJobResult) {
				$count++;
				if ($this->exitCode !== null) {
					break;
				}

			} elseif ($worker) {
				if ($this->exitCode !== null) {
					break;
				}
				sleep($sleep);

			} else {
				break;
			}
		}

		$this->logger->info(sprintf('%s: finished', $this->getName()), [
			'processedCount' => $count,
			'exitCode' => $this->exitCode,
		]);

		return $this->exitCode ?: 0;
	}


	protected function handleSignal(int $signalNumber): void
	{
		$this->logger->info(sprintf('%s: received signal %d', $this->getName(), $signalNumber));
		$this->exitCode = 128 + $signalNumber;
	}


	/**
	 * @return bool returns true if there was a job to do, or false if there was nothing to do
	 */
	abstract protected function processSingleJob(InputInterface $input): bool;


	private function riskOfNotEnoughMemory(): bool
	{
		$currentMemoryUsage = memory_get_usage();
		$currentMemoryLimit = $this->getMemoryLimitInBytes();

		if ($currentMemoryLimit > 0 && $currentMemoryUsage > self::MEMORY_LIMIT_MAX_CONSUMPTION * $currentMemoryLimit) {
			$this->logger->error(sprintf('%s: consumed over %d %% of memory limit', $this->getName(), self::MEMORY_LIMIT_MAX_CONSUMPTION * 100), [
				'currentMemoryUsage' => $currentMemoryUsage,
				'currentMemoryLimit' => $currentMemoryLimit,
			]);

			return true;
		}

		return false;
	}


	private function getMemoryLimitInBytes(): int
	{
		$memoryLimit = ini_get('memory_limit') ?: '-1';
		$max = intval($memoryLimit, 0);

		$unit = strtolower(substr($memoryLimit, -1));
		$shift = ['k' => 10, 'm' => 20, 'g' => 30, 't' => 40][$unit] ?? 0;

		return $max << $shift;
	}
}
