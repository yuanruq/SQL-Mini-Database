/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ThrowsTag;
import com.sun.javadoc.Type;

class HidingThrowsTagWrapper extends HidingTagWrapper implements ThrowsTag {
    public HidingThrowsTagWrapper(ThrowsTag thrtag, Map mapWrappers) {
        super(thrtag, mapWrappers);
    }

    private ThrowsTag _getThrowsTag() {
        return (ThrowsTag)getWrappedObject();
    }

    public ClassDoc exception() {
        return (ClassDoc)wrapOrHide(_getThrowsTag().exception());
    }

    public String exceptionComment() {
        return _getThrowsTag().exceptionComment();
    }

    public String exceptionName() {
        return _getThrowsTag().exceptionName();
    }

    public Type exceptionType() {
        return (Type)wrapOrHide(_getThrowsTag().exceptionType());
    }
}
