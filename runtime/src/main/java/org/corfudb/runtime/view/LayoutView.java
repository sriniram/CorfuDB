package org.corfudb.runtime.view;

import static java.util.Arrays.stream;

import lombok.extern.slf4j.Slf4j;
import org.corfudb.protocols.wireprotocol.LayoutPrepareResponse;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.exceptions.AlreadyBootstrappedException;
import org.corfudb.runtime.exceptions.NetworkException;
import org.corfudb.runtime.exceptions.NoBootstrapException;
import org.corfudb.runtime.exceptions.OutrankedException;
import org.corfudb.runtime.exceptions.QuorumUnreachableException;
import org.corfudb.runtime.exceptions.WrongClusterException;
import org.corfudb.runtime.exceptions.WrongEpochException;
import org.corfudb.util.CFUtils;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * Created by mwei on 12/10/15.
 */
@Slf4j
public class LayoutView extends AbstractView {

    public LayoutView(@Nonnull final CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Retrieves current layout.
     **/
    public Layout getLayout() {
        return layoutHelper(RuntimeLayout::getLayout);
    }

    public RuntimeLayout getRuntimeLayout() {
        return layoutHelper(l -> l);
    }

    public RuntimeLayout getRuntimeLayout(@Nonnull Layout layout) {
        return new RuntimeLayout(layout, runtime);
    }

    /**
     * Retrieves the number of nodes needed to obtain a quorum.
     * We define a quorum for the layout view as n/2+1
     *
     * @return The number of nodes required for a quorum.
     */
    public int getQuorumNumber() {
        return (getLayout().getLayoutServers().size() / 2) + 1;
    }

    /**
     * Drives the consensus protocol for persisting the new Layout.
     * TODO currently the code can drive only one Layout change.
     * If it has to drive a previously incomplete round
     * TODO it will drop it's own set of changes. Need to revisit this.
     * A change of layout proposal consists of a rank and the desired layout.
     *
     * @param layout The layout to propose.
     * @param rank The rank for the proposed layout.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     * @throws WrongEpochException wrong epoch number.
     */
    public void updateLayout(Layout layout, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {
        // Note this step is done because we have added the layout to the Epoch.
        long newEpoch = layout.getEpoch();
        Layout currentLayout = getLayout();

        if (currentLayout.getEpoch() != newEpoch - 1) {
            log.error("updateLayout: Runtime layout has epoch {} but expected {} to move to {}",
                    currentLayout.getEpoch(), newEpoch - 1, newEpoch);
            throw new WrongEpochException(newEpoch - 1);
        }

        if (currentLayout.getClusterId() != null
            && !currentLayout.getClusterId().equals(layout.getClusterId())) {
            log.error("updateLayout: Requested layout has cluster Id {} but expected {}",
                    layout.getClusterId(), currentLayout.getClusterId());
            throw new WrongClusterException(currentLayout.getClusterId(), layout.getClusterId());
        }

        //phase 1: prepare with a given rank.
        Layout acceptedLayout = prepare(newEpoch, rank);
        Layout layoutToPropose = acceptedLayout != null ? acceptedLayout : layout;
        //phase 2: propose the new layout.
        propose(newEpoch, rank, layoutToPropose);
        //phase 3: committed
        committed(newEpoch, layoutToPropose);
    }

    /**
     * Sends prepare to current layout servers and can proceed only if it is promised by a quorum.
     *
     * @param epoch The epoch for the new consensus.
     * @param rank The rank for the proposed layout.
     * @return layout Accepted layout with the highest rank from acceptors or null.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     * @throws OutrankedException outranked exception, i.e., higher rank.
     * @throws WrongEpochException wrong epoch number.
     */
    @SuppressWarnings("unchecked")
    public Layout prepare(long epoch, long rank)
            throws QuorumUnreachableException, OutrankedException, WrongEpochException {
        CompletableFuture<LayoutPrepareResponse>[] prepareList = getLayout().getLayoutServers()
                .stream()
                .map(x -> {
                    CompletableFuture<LayoutPrepareResponse> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        cf = getRuntimeLayout().getLayoutClient(x).prepare(epoch, rank);
                    } catch (Exception e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);

        long promises = 0L;
        long timeouts = 0L;
        long outranks = 0L;
        long wrongEpochRejected = 0L;
        long outrankedRecord = Long.MIN_VALUE;
        for (CompletableFuture cf : prepareList) {
            if (promises >= getQuorumNumber()) {
                break;
            }

            try {
                CFUtils.getUninterruptibly(cf, OutrankedException.class, TimeoutException.class,
                        NetworkException.class, WrongEpochException.class);
                promises++;
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            } catch (OutrankedException oe) {
                outranks++;
                outrankedRecord = Long.max(outrankedRecord, oe.getNewRank());
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }
        }

        // when quorum is unreachable, check if outranks is majority and throw OutrankedException
        if (promises < getQuorumNumber()) {
            log.debug("prepare: Quorum unreachable, promises={}, required={}, timeouts={}, " +
                            "wrongEpochRejected={}, outranks={}",
                    promises, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);
            if (outranks >= getQuorumNumber()) {
                throw new OutrankedException(outrankedRecord);
            } else {
                throw new QuorumUnreachableException(Math.toIntExact(promises), getQuorumNumber());
            }
        }

        log.debug("prepare: Successful responses={}, needed={}, timeouts={}, " +
                        "wrongEpochRejected={}, outranks={}",
                promises, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);

        // Collect any layouts that have been proposed before.
        List<LayoutPrepareResponse> acceptedLayouts = stream(prepareList)
                .filter(cf -> cf.isDone() && !cf.isCompletedExceptionally())
                .map(cf -> cf.getNow(null))
                .filter(x -> x != null && x.getLayout() != null)
                .collect(Collectors.toList());

        // Return accepted layout with the highest rank or null.
        if (acceptedLayouts.isEmpty()) {
            return null;
        } else {
            // Choose the layout with the highest rank proposed before.
            long highestRank = Long.MIN_VALUE;
            Layout layoutWithHighestRank = null;
            for (LayoutPrepareResponse layoutPrepareResponse : acceptedLayouts) {
                if (layoutPrepareResponse.getRank() > highestRank) {
                    highestRank = layoutPrepareResponse.getRank();
                    layoutWithHighestRank = layoutPrepareResponse.getLayout();
                }
            }
            return layoutWithHighestRank;
        }
    }

    /**
     * Proposes new layout to all the servers in the current layout,
     * and can proceed only if it is accepted by a quorum.
     *
     * @param epoch The epoch for the new consensus.
     * @param rank The rank for the proposed layout.
     * @param layout The layout to propose.
     *
     * @throws QuorumUnreachableException Thrown if responses not received from a majority of
     *                                    layout servers.
     */
    @SuppressWarnings("unchecked")
    public Layout propose(long epoch, long rank, Layout layout)
            throws QuorumUnreachableException, OutrankedException {
        CompletableFuture<Boolean>[] proposeList = getLayout().getLayoutServers().stream()
                .map(x -> getRuntimeLayout().getLayoutClient(x).propose(epoch, rank, layout))
                .toArray(CompletableFuture[]::new);

        long accepts = 0L;
        long timeouts = 0L;
        long outranks = 0L;
        long wrongEpochRejected = 0L;
        for (CompletableFuture cf : proposeList) {
            if (accepts >= getQuorumNumber()) {
                break;
            }

            try {
                CFUtils.getUninterruptibly(cf, OutrankedException.class, TimeoutException.class,
                        NetworkException.class, WrongEpochException.class);
                accepts++;
            } catch (TimeoutException | NetworkException e) {
                timeouts++;
            } catch (OutrankedException oe) {
                outranks++;
            } catch (WrongEpochException we) {
                wrongEpochRejected++;
            }
        }

        if (accepts < getQuorumNumber()) {
            log.debug("propose: Quorum unreachable, accepts={}, required={}, timeouts={}, " +
                            "wrongEpochRejected={}, outranks={}",
                    accepts, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);
            throw new QuorumUnreachableException(Math.toIntExact(accepts), getQuorumNumber());
        }

        log.debug("propose: Successful responses={}, needed={}, timeouts={}, "
                        + "wrongEpochRejected={}, outranks={}",
                accepts, getQuorumNumber(), timeouts, wrongEpochRejected, outranks);
        return layout;
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * TODO Current policy is to send the committed layout once. Need to revisit this in order
     * TODO to drive the new layout to all the involved LayoutServers.
     * TODO The new layout servers are not bootstrapped and will reject committed messages.
     * TODO Need to fix this.
     *
     * @throws WrongEpochException wrong epoch number.
     */
    public void committed(long epoch, Layout layout) {
        committed(epoch, layout, false);
    }

    /**
     * Send committed layout to the old Layout servers and the new Layout Servers.
     * If force is true, then the layout forced on all layout servers.
     */
    @SuppressWarnings("unchecked")
    public void committed(long epoch, Layout layout, boolean force)
            throws WrongEpochException {
        CompletableFuture<Boolean>[] commitList = layout.getLayoutServers().stream()
                .map(x -> {
                    CompletableFuture<Boolean> cf = new CompletableFuture<>();
                    try {
                        // Connection to router can cause network exception too.
                        if (force) {
                            cf = getRuntimeLayout(layout).getLayoutClient(x).force(layout);
                        } else {
                            cf = getRuntimeLayout(layout).getLayoutClient(x).committed(epoch, layout);
                        }
                    } catch (NetworkException e) {
                        cf.completeExceptionally(e);
                    }
                    return cf;
                })
                .toArray(CompletableFuture[]::new);


        int responses = 0;
        for (CompletableFuture cf : commitList) {
            try {
                CFUtils.getUninterruptibly(cf, WrongEpochException.class,
                        TimeoutException.class, NetworkException.class, NoBootstrapException.class);
                responses++;
            } catch (WrongEpochException e) {
                if (!force) {
                    throw e;
                }
                log.warn("committed: encountered exception", e);
            } catch (NoBootstrapException |  TimeoutException | NetworkException e) {
                log.warn("committed: encountered exception", e);
            }
        }
        log.debug("committed: Successful requests={}, responses={}", commitList.length, responses);
    }

    /**
     * Bootstraps the layout server of the specified node.
     * If already bootstrapped, it completes silently.
     *
     * @param endpoint Endpoint to bootstrap.
     * @param layout   Layout to bootstrap with.
     * @return Completable Future which completes with True when the layout server is bootstrapped.
     */
    CompletableFuture<Boolean> bootstrapLayoutServer(@Nonnull String endpoint, @Nonnull Layout layout) {
        return getRuntimeLayout(layout).getLayoutClient(endpoint).bootstrapLayout(layout)
                .exceptionally(throwable -> {
                    try {
                        CFUtils.unwrap(throwable, AlreadyBootstrappedException.class);
                    } catch (AlreadyBootstrappedException e) {
                        log.info("bootstrapLayoutServer: Layout Server {} already bootstrapped.", endpoint);
                    }
                    return true;
                })
                .thenApply(result -> {
                    log.info("bootstrapLayoutServer: Layout Server {} bootstrap successful", endpoint);
                    return true;
                });
    }
}
