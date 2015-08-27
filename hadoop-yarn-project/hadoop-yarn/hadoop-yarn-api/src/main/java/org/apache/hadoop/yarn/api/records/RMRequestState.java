package org.apache.hadoop.yarn.api.records;

import org.apache.hadoop.classification.InterfaceAudience.Public;
import org.apache.hadoop.classification.InterfaceStability.Stable;

/**
 * Created by shashi_ibm on 19-Aug-15.
 */
@Public
@Stable
public enum RMRequestState {
    /** Resource manager has requested dynamic model evaluation for ML jobs **/
    RMRequest,
    /** Resource manager has not requested dynamic model evaluation for ML jobs yet **/
    NoRMRequest
}
