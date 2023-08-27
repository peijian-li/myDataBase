package transaction;

import common.Permissions;
import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Lock {

    private TransactionId transactionId;
    private Permissions permissions;


}
