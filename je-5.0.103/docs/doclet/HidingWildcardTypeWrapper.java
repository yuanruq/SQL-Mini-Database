/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.Type;
import com.sun.javadoc.WildcardType;

class HidingWildcardTypeWrapper extends HidingTypeWrapper
                                implements WildcardType {
    public HidingWildcardTypeWrapper(WildcardType type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private WildcardType _getWildcardType() {
        return (WildcardType)getWrappedObject();
    }

    public Type[] extendsBounds() {
        return (Type[])wrapOrHide(_getWildcardType().extendsBounds());
    }

    public Type[] superBounds() {
        return (Type[])wrapOrHide(_getWildcardType().superBounds());
    }
}
